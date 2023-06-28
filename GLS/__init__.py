import argparse
import concurrent.futures
import json
import numpy
import os
import sys
import threading
import time

import Google.GSuite


class GDriveList(object):

    records = list()
    auth = None
    file_counter = 0
    savepath = None
    max_rec = 1200
    rec_lock = threading.Lock()
    file_lock = threading.Lock()

    def __init__(
        self, profile_name: str = "default", username: str = None,
        savepath: str = None, max_records: int = 0
    ):
        json_result = {}
        uhome = os.path.expanduser("~")
        auth_path = f"{uhome}/.gws/{profile_name}.json"
        self.cfg_path = f"{uhome}/.gws/{profile_name}.cfg"

        with open(auth_path, "r") as f:
            self.auth = json.load(f)
            self.gsuite = Google.GSuite.Client(**self.auth)
        if os.path.isfile(self.cfg_path):
            with open(self.cfg_path, "r") as cfg_fd:
                self.cfg = json.load(cfg_fd)
        else:
            self.cfg = {}
        if username:
            self.cfg["username"] = username
        elif "username" in self.cfg:
            username = self.cfg["username"]
        self.gsuite.username = self.cfg["username"]
        self.savepath = savepath
        self.max_rec = int(max_records)

    def start(self, domain: str = None, max_threads: int = 4):
        if domain:
            self.cfg["domain"] = domain
        else:
            domain = self.cfg["domain"]

        with open(self.cfg_path, "w") as cfg_fd:
            cfg_fd.write(json.dumps(self.cfg, indent=4))
        users = list()
        page_token = None
        print("Get user list")
        while True:
            res = self.gsuite.directory.users.list(
                domain=domain, page_token=page_token
            )
            users.extend(res["users"])
            if "nextPageToken" in res:
                page_token = res["nextPageToken"]
            else:
                break
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_threads
        ) as exc:
            exc.map(self.get_files, users)

        out_rec = self.get_max_records()
        if len(out_rec) > 0:
            self.file_save(records=out_rec)

    def get_files(self, user: dict):
        email = user["primaryEmail"]
        print(f"User [{email}]: process start")
        gdrive = Google.GSuite.Client(**self.auth)
        gdrive.username = email
        files = list()
        page_token = None
        owners = ["displayName", "emailAddress"]
        owners = f"owners({','.join(owners)})"
        permissions = ["displayName", "type", "emailAddress", "role", "domain"]
        permissions = f"permissions({','.join(permissions)})"
        f = [
            "name", "id", owners, "createdTime", "modifiedTime",
            "sharedWithMeTime", "parents", "driveId", permissions, "sharingUser(emailAddress)"
        ]
        fields = f"nextPageToken,files({','.join(f)})"
        retry = 0
        while True:
            try:
                res = gdrive.drive.files.list(
                    page_token=page_token, page_size=1000, fields=fields,
                    include_items_from_all_drives=True,
                    supports_all_drives=True
                )
                files = list(map(self.make_record, res["files"]))
                num = self.add_records(files)
                if num >= self.max_rec:
                    out_rec = self.get_max_records()
                    self.file_save(records=out_rec)
                retry = 0
                if "nextPageToken" in res:
                    print(f"User [{email}]: get next page")
                    page_token = res["nextPageToken"]
                    time.sleep(0.1)
                else:
                    break
            except Exception as e:
                print(e)
                if retry == 3:
                    break
                else:
                    retry += 1

        print(f"User [{email}]: process end")
        return files

    def get_max_records(self):
        with self.rec_lock:
            ret = self.records.copy()
            ret = ret[:self.max_rec]
            self.records = self.records[self.max_rec:]
        return ret

    def file_save(self, records):
        with self.file_lock:
            base, ext = os.path.splitext(self.savepath)
            self.file_counter += 1
            savepath = f"{base}_{str(self.file_counter).zfill(3)}{ext}"
            with open(savepath, "w") as f:
                f.write("\n".join(records))
            print(f"File out [{savepath}]")

    def add_records(self, files):
        with self.rec_lock:
            files = list(set(files))
            self.records.extend(files)
            num = len(self.records)
        return num

    def make_record(self, file):
        sep = "|"
        try:
            owners = file.pop("owners", [{}])
            owner_displayname = list()
            owner_emailaddress = list()
            for owner in owners:
                dn = owner.get('displayName', None)
                if dn:
                    owner_displayname.append(dn)
                ea = owner.get('emailAddress', None)
                if ea:
                    owner_emailaddress.append(ea)
            file["owner_displayName"] = sep.join(owner_displayname)
            file["owner_emailAddress"] = sep.join(owner_emailaddress)
            sharing_user = file.pop("sharingUser", {})
            file["sharingUserEmailAddress"] = sharing_user.pop("emailAddress", "")

            for key in ["permissions", "parents"]:
                data = file.pop(key, [])
                data = json.dumps(data, indent=4, ensure_ascii=False)
                data = data.replace('"', '""')
                file[key] = data
        except Exception as e:
            raise e
        return ",".join([
            f'"{file.get("name", "")}"',
            f'"{file.get("id", "")}"',
            f'"{file.get("owner_displayName", "")}"',
            f'"{file.get("owner_emailAddress", "")}"',
            f'"{file.get("createdTime", "")}"',
            f'"{file.get("modifiedTime", "")}"',
            f'"{file.get("sharedWithMeTime", "")}"',
            f'"{file.get("parents", "")}"',
            f'"{file.get("driveId", "")}"',
            f'"{file.get("permissions", "")}"',
            f'"{file.get("sharingUserEmailAddress", "")}"',
        ])


def get_args():
    parser = argparse.ArgumentParser(
        prog="gls",
        description="Google WorkSpace ドライブファイル一覧出力ツール ver.1.0.5",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--domain', '-d', help='Google WorkSpaceのドメイン名を指定します'
    )
    parser.add_argument(
        '--user', '-u', help='Google WorkSpaceの実行ユーザー名を指定します'
    )
    parser.add_argument(
        '--profile', '-p', help='読込プロファイル', default="default"
    )

    parser.add_argument(
        '--threads', '-t', help='最大スレッド数', default=os.cpu_count()
    )
    parser.add_argument(
        '--line', '-l', help='出力するCSVファイルの最大行数', default=1000000
    )
    parser.add_argument(
        '--savepath', '-s', help='保存先パス', default="gdrive_files.csv"
    )
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    gls = GDriveList(
        profile_name=args.profile, username=args.user,
        savepath=args.savepath, max_records=int(args.line)
    )
    gls.start(domain=args.domain, max_threads=int(args.threads))


if __name__ == '__main__':
    main()
