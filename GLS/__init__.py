import argparse
import concurrent.futures
import json
import numpy
import os
import sys

import Google.GSuite


class GDriveList(object):

    records = list()
    auth = None

    def __init__(self, profile_name: str = "default", username: str = None):
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

    def start(self, domain: str = None):
        if domain:
            self.cfg["domain"] = domain
        else:
            domain = self.cfg["domain"]
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
        num = len(users)
        files = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num) as exc:
            for recs in exc.map(self.get_files, users):
                files.extend(recs)
        print("Duplicate delete process")
        records = list(set(files))
        max_rec = 1000000
        num = len(records)
        for i in range(0, num, max_rec):
            self.records.append(records[i:i+max_rec])

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
            "sharedWithMeTime", "parents", "driveId", permissions
        ]
        fields = f"nextPageToken,files({','.join(f)})"
        while True:
            res = self.gsuite.drive.files.list(
                page_token=page_token, page_size=1000, fields=fields,
                include_items_from_all_drives = True,
                supports_all_drives = True
            )
            files.extend(res["files"])
            if "nextPageToken" in res:
                page_token = res["nextPageToken"]
            else:
                break
        files = list(map(self.make_record, files))
        print(f"User ({email}): process end")
        return files

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
        ])

    def save(self, savepath: str = None):
        count = 1
        for records in self.records:
            result = "\n".join(records)
            if len(self.records) > 1:
                base, ext = os.path.splitext(savepath)
                s_path = f"{base}_{str(count).zfill(3)}{ext}"
            else:
                s_path = savepath
            print(f"{s_path}に保存")
            with open(s_path, "w") as f:
                f.write(result)
            count = count + 1


def get_args():
    parser = argparse.ArgumentParser(
        description="Google WorkSpace ドライブファイル一覧出力ツール",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--domain', '-d', help='Google WorkSpaceのドメイン名を指定します')
    parser.add_argument('--user', '-u', help='Google WorkSpaceの実行ユーザー名を指定します')
    parser.add_argument('--profile', '-p', help='読込プロファイル', default="default")
    parser.add_argument(
        '--savepath', '-s', help='保存先パス', default="gdrive_files.csv"
    )
    args = parser.parse_args()
    return args


def main():
    args = get_args()

    gls = GDriveList(profile_name=args.profile, username=args.user)
    gls.start(domain=args.domain)
    gls.save(savepath=args.savepath)


if __name__ == '__main__':
    main()
