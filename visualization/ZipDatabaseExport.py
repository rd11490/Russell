import os
import shutil

root_path = "/Users/ryandavis/Documents/Workspace/NBAData/"
csv_path = "{0}/{1}".format(root_path, "csv")
sql_path = "{0}/{1}".format(root_path, "sql/Individual Tables")


def zip_all(path):
    path = os.path.abspath(os.path.normpath(os.path.expanduser(path)))
    for folder in os.listdir(path):
        if ".zip" not in folder and ".DS_Store" not in folder:
            out_file_name = "{0}/{1}".format(path, folder)
            item_to_zip_name = "{0}/{1}".format(path, folder)
            if os.path.isfile(item_to_zip_name):
                new_folder_name = item_to_zip_name.replace(".sql", "_folder")
                if not os.path.exists(new_folder_name):
                    os.mkdir(new_folder_name)
                shutil.copy(item_to_zip_name, new_folder_name)
                item_to_zip_name = new_folder_name
            shutil.make_archive(out_file_name, 'zip', item_to_zip_name)

def delete_zips(path):
    path = os.path.abspath(os.path.normpath(os.path.expanduser(path)))
    for folder in os.listdir(path):
        if ".zip" in folder or "_folder" in folder:
            full_path = "{0}/{1}".format(path, folder)
            if os.path.isdir(full_path):
                shutil.rmtree(full_path)
            else:
                os.remove(full_path)


print("Deleting Zips and Folders from SQL databases")
delete_zips(sql_path)
print("Deleting Zips and Folders from CSV")
delete_zips(csv_path)
print("Rezipping Sql Backups")
zip_all(sql_path)
print("Rezipping CSV Backups")
zip_all(csv_path)
