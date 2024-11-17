import os
import pandas as pd
import yaml
import shutil
import time


def create_folder_dataframe(folder_path):
    """
    Creates a DataFrame containing file information for a given folder path.

    Args:
        folder_path (str): The path of the folder to scan.

    Returns:
        dict: A dictionary containing the DataFrame with file information.
    """
    data = []
    for root, dirs, files in os.walk(folder_path):
        for name in files:
            file_path = os.path.join(root, name)
            changed_time = os.path.getmtime(file_path)
            data.append({"name": name, "path": file_path, "changed": changed_time})
    
    df = pd.DataFrame(data, columns=["name", "path", "changed"])
    df["changed"] = pd.to_datetime(df["changed"], unit='s')
    df["path"] = df["path"].apply(lambda x: x.replace(folder_path, ""))

    return {"dataframe": df}


def load_config(config_path):
    """
    Loads the configuration from a YAML file.

    Args:
        config_path (str): The path to the configuration file.

    Returns:
        dict: The configuration dictionary.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def copy_files_to_backup(df, source_folder, target_folder):
    """
    Copies files from the source folder to the target folder based on the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing file paths to copy.
        source_folder (str): The source folder path.
        target_folder (str): The target folder path.
    """
    for index, row in df.iterrows():
        source_path = os.path.join(source_folder, row['path'].lstrip("\\"))
        target_path = os.path.join(target_folder, row['path'].lstrip("\\"))
        
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.copy2(source_path, target_path)


def main():
    """
    Main function to execute the backup process.
    """
    config = load_config("config/config.yml")

    wrkdir_files = create_folder_dataframe(config["backup_config"]["source_filepath"])
    bckpdir_files = create_folder_dataframe(config["backup_config"]["target_filepath"])

    df_wrkdir = wrkdir_files["dataframe"]
    df_bckpdir = bckpdir_files["dataframe"]

    new_df = df_wrkdir[~df_wrkdir['path'].isin(df_bckpdir['path'])]
    changed_df = df_wrkdir[df_wrkdir['path'].isin(df_bckpdir['path'])]
    changed_df = changed_df[changed_df.apply(lambda x: x['changed'] > df_bckpdir[df_bckpdir['path'] == x['path']]['changed'].values[0], axis=1)]
    del_df = df_bckpdir[~df_bckpdir['path'].isin(df_wrkdir['path'])]

    print("\n ---------------- \n")

    if new_df.shape[0] > 0:
        print("New files to backup:")
        print(new_df)
        copy_files_to_backup(new_df, config["backup_config"]["source_filepath"], config["backup_config"]["target_filepath"])
        print("Files copied")
    else:
        print("No files to backup")

    print("\n ---------------- \n")

    if changed_df.shape[0] > 0:
        print("Changed files to backup:")
        print(changed_df)
        copy_files_to_backup(changed_df, config["backup_config"]["source_filepath"], config["backup_config"]["target_filepath"])
        print("Files copied")
    else:
        print("No changed files to backup")

    print("\n ---------------- \n")

    if del_df.shape[0] > 0:
        print("Files to delete:")
        print(del_df)
        for index, row in del_df.iterrows():
            target_path = os.path.join(config["backup_config"]["target_filepath"], row['path'].lstrip("\\"))
            os.remove(target_path)
        print("Files deleted")
    else:    
        print("No files to delete")

    print("\n ---------------- \n")
    
    print("End of the program")
    time.sleep(5)


if __name__ == "__main__":
    main()