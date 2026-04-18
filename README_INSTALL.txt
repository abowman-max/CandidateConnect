Candidate Connect update v3

Replace these files in your GitHub project folder:
- app.py
- candidate_connect_pdf_report.py
- requirements.txt (only if your project does not already include reportlab)

Keep these in the same folder:
- candidate_connect_logo.png
- TSS_Logo_Transparent.png

What changed in v3:
- Removed the on-screen Preview section
- Removed the on-screen door-to-door preview grid
- Added Download Filtered CSV option
- Mail, Texting, Filtered CSV, Door-to-Door Excel, and Door-to-Door PDF are now prepared only after their own button is clicked
- This reduces page-load work and should make the app feel much faster

After upload:
1. Commit/push to GitHub
2. Redeploy Streamlit
3. Open the app
4. Apply filters
5. Click only the export you want to prepare
