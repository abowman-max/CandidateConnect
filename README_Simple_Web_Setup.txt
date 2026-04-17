Candidate Connect Web Version - Simple Setup

Your Google Drive parquet file is already built into app.py.

VERY IMPORTANT BEFORE DEPLOYING:
1. Open your data.parquet file in Google Drive.
2. Click Share.
3. Change access so anyone with the link can view.
4. Leave the file in Google Drive.

FILES TO PUT IN GITHUB:
- app.py
- requirements.txt
- pdf_utils.py
- version.txt
- candidate_connect_logo.png (optional but recommended)
- TSS_Logo_Transparent.png (optional but recommended)

VERY SIMPLE STEP-BY-STEP:
1. Sign in to GitHub.
2. Create a new repository. A private repository is fine.
3. Upload the files listed above into that repository.
4. Go to Streamlit Community Cloud.
5. Sign in with GitHub.
6. Click Create app.
7. Pick your repository.
8. For Main file path, choose app.py
9. Click Deploy.
10. Wait for the app to finish building.
11. Open the web link Streamlit gives you.
12. Test the app in your browser.

IF THE APP FAILS TO LOAD DATA:
- Make sure the Google Drive file is shared so it can be downloaded by link.
- Then go back to Streamlit and click Reboot app.

WHAT IS DIFFERENT IN THIS WEB VERSION:
- Data loads from Google Drive.
- Finished files stay in the app session and download directly from the browser.
- No Mac installer or desktop packaging is needed.

WHAT TO SEND YOUR PARTNER:
- The Streamlit app web link

