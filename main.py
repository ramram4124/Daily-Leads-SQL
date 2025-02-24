import psycopg2
from tabulate import tabulate
import pandas as pd
from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Database connection parameters from environment variables
db_params = {
    'dbname': os.environ.get('DB_NAME', 'postgres'),
    'user': os.environ.get('DB_USER', 'postgres.qcvfmiqzkfhinxlhknnd'),
    'password': os.environ.get('DB_PASSWORD', 'gaadimech123'),
    'host': os.environ.get('DB_HOST', 'aws-0-ap-south-1.pooler.supabase.com'),
    'port': os.environ.get('DB_PORT', '6543')
}

def send_email(table_html):
    sender_email = os.environ.get('EMAIL_SENDER')
    receiver_email = os.environ.get('EMAIL_RECEIVER')
    password = os.environ.get('EMAIL_PASSWORD')

    # Add debug prints
    print(f"Sending email from: {sender_email}")
    print(f"Sending email to: {receiver_email}")
    print(f"Password length: {len(password) if password else 0}")

    msg = MIMEMultipart()
    msg['Subject'] = f'Lead Report - {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    msg['From'] = sender_email
    msg['To'] = receiver_email

    html_content = f"""
    <html>
        <body>
            <h2>Lead Report</h2>
            {table_html}
        </body>
    </html>
    """
    
    msg.attach(MIMEText(html_content, 'html'))

    try:
        print("Attempting to connect to SMTP server...")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            print("Connected to SMTP server")
            server.login(sender_email, password)
            print("Logged in successfully")
            server.send_message(msg)
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        # Print more detailed error information
        import traceback
        print(traceback.format_exc())

def fetch_user_leads_data():
    try:
        # Establish connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Your SQL query
        query = """
        SELECT 
            u.username,
            SUM(CASE 
                WHEN DATE(l.created_at) = CURRENT_DATE THEN 1 
                ELSE 0 
            END) as leads_created_today,
            
            SUM(CASE 
                WHEN DATE(l.modified_at) = CURRENT_DATE 
                AND DATE(l.created_at) != DATE(l.modified_at) THEN 1 
                ELSE 0 
            END) as leads_modified_today,
            
            SUM(CASE 
                WHEN DATE(l.created_at) = CURRENT_DATE THEN 1 
                WHEN DATE(l.modified_at) = CURRENT_DATE 
                AND DATE(l.created_at) != DATE(l.modified_at) THEN 1 
                ELSE 0 
            END) as total_calls
        FROM "lead" l
        JOIN "user" u ON l.creator_id = u."id"
        GROUP BY u.username
        HAVING SUM(CASE 
            WHEN DATE(l.created_at) = CURRENT_DATE THEN 1 
            WHEN DATE(l.modified_at) = CURRENT_DATE 
            AND DATE(l.created_at) != DATE(l.modified_at) THEN 1 
            ELSE 0 
        END) > 0
        ORDER BY total_calls DESC;
        """
        
        # Execute query
        cursor.execute(query)
        
        # Fetch results
        results = cursor.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Create DataFrame
        df = pd.DataFrame(results, columns=columns)
        
        # Generate both console output and HTML
        print("\nResults for", datetime.now().strftime("%Y-%m-%d"), ":\n")
        print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
        
        # Generate HTML table and send email
        html_table = df.to_html(index=False, classes='table table-striped')
        send_email(html_table)
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    fetch_user_leads_data()

    #oqenmtwayihewaws
