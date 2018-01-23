import os
import zipfile

root_path = "/Users/ryandavis/Documents/Workspace/NBAData/"
csv_path = "{0}/{1}".format(root_path, "csv")
sql_path = "{0}/{1}".format(root_path, "sql/Individual Tables")


def zip_all(path):
    path = os.path.abspath(os.path.normpath(os.path.expanduser(path)))
    for folder in os.listdir(path):
        if (".zip" not in folder and ".DS_Store" not in folder):
            filename = "{0}/{1}.zip".format(path,folder)
            with zipfile.ZipFile(filename, "w", zipfile.ZIP_DEFLATED) as zip:
                for root, dirs, files in os.walk(folder):
                    # add directory (needed for empty dirs)
                    zip.write(root, os.path.relpath(root, folder))
                    for file in files:
                        filename = os.path.join(root, file)
                        if os.path.isfile(filename):  # regular files only
                            arcname = os.path.join(os.path.relpath(root, folder), file)
                            zip.write(filename, arcname)



zip_all(sql_path)
zip_all(csv_path)