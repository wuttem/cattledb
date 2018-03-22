
import os
from cattledb.server import create_app

os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8080"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/mnt/c/Users/mths/.ssh/google_gcp_credentials.json"
app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, workers=4)