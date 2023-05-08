import argparse
import concurrent.futures
import json
import numpy
import os
import sys

import Google.GSuite


class GDriveList(object):

    records = list()

    def __init__(self, profile_name: str = "default", username: str = None):
        json_result = {}
        uhome = os.path.expanduser("~")
        auth_path = f"{uhome}/.gws/{profile_name}.json"
        self.cfg_path = f"{uhome}/.gws/{profile_name}.cfg"

        with open(auth_path, "r") as f:
            auth = json.load(f)
            self.gsuite = Google.GSuite.Client(**auth)
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

    def start(self):
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
        records = list(map(self.make_record, files))
        max_rec = 1000000
        num = len(records)
        for i in range(0, num, max_rec):
            self.records.append(records[i:i+max_rec])

    def make_record(self, file):
        sep = "|"
        try:
            for key in ["owners", "permissions", "parents"]:
                data = file.pop(key, [])
                data = json.dumps(data, indent=4, ensure_ascii=False)
                data = data.replace('"', '""')
                file[key] = data
        except Exception as e:
            raise e
        return ",".join([
            f'"{file.get("name", "")}"',
            f'"{file.get("id", "")}"',
            f'"{file.get("owners", "")}"',
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
            if savepath is not None:
                if len(self.records) > 1:
                    base, ext = os.path.splitext(savepath)
                    s_path = f"{base}_{str(count).zfill(3)}{ext}"
                else:
                    s_path = savepath
                with open(s_path, "w") as f:
                    f.write(result)
            count = count + 1


def get_args():
    parser = argparse.ArgumentParser(
        description="Google WorkSpace ドライブファイル一覧出力ツール",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--user', '-u', help='Google WorkSpaceの実行ユーザー名を指定します')
    parser.add_argument('--profile', '-p', help='読込プロファイル', default="default")
    parser.add_argument(
        '--savepath', '-s', help='保存先パス'
    )
    args = parser.parse_args()
    return args


def main():
    args = get_args()

    gls = GDriveList(profile_name=args.profile, username=args.user)
    gls.start()
    gls.save(savepath=args.savepath)


if __name__ == '__main__':
    main()
