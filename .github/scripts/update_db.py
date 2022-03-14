import requests
import sys
import json
import time

# update Databricks branch
db_instance, db_auth_token, db_repo_folder, db_repo_id, gh_repo, gh_branch = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]

print('db_instance: ' + db_instance)
print('db_repo_folder: ' + db_repo_folder)
print('db_repo_id: ' + db_repo_id)
print('gh_repo: ' + gh_repo)
print('gh_branch: ' + gh_branch)

endpoint = f"{db_instance}api/2.0/repos/{db_repo_id}"
headers = {"Authorization": f"Bearer {db_auth_token}"}
data = {'branch': f'{gh_branch}'}

databricks_response = requests.patch(endpoint, headers = headers, json = data)
print(databricks_response.status_code)
print(databricks_response.json())
if databricks_response.status_code == 200:
    print("success")
else:
    raise Exception("Branch Not Updated")

