import subprocess
import os

def run_command(command):
    try:
        subprocess.run(command, check=True, shell=True)
        print(f"Command executed successfully: {' '.join(command)}")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")

def main():
    # Define the path to the PostgreSQL bin directory
    pg_bin_path = r"C:\Program Files\PostgreSQL\16\bin"

    # Change the current working directory to the PostgreSQL bin directory
    os.chdir(pg_bin_path)

    # Commands to be executed
    start_primary_db = r"pg_ctl -D /tmp/primary_db start"
    restart_replica_db = r"pg_ctl -D /tmp/replica_db start"

    # Run the commands
    run_command(start_primary_db)
    run_command(restart_replica_db)

if __name__ == "__main__":
    main()
