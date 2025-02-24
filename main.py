import psycopg2
from tabulate import tabulate
import pandas as pd
from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/execution.log')
    ]
)

# Database connection parameters from environment variables
db_params = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'sslmode': 'require',
    'sslrootcert': '/etc/ssl/certs/ca-certificates.crt' 
}

def check_environment_variables():
    required_vars = [
        'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT',
        'EMAIL_SENDER', 'EMAIL_RECEIVER', 'EMAIL_PASSWORD'
    ]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logging.error("Missing required environment variables:")
        for var in missing_vars:
            logging.error(f"- {var}")
        return False
    logging.info("All required environment variables are set")
    return True

def send_email(table_html):
    sender_email = os.environ.get('EMAIL_SENDER')
    receiver_email = os.environ.get('EMAIL_RECEIVER')
    password = os.environ.get('EMAIL_PASSWORD')

    logging.info(f"Sending email from: {sender_email} to: {receiver_email}")

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
        logging.info("Connecting to SMTP server...")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            logging.info("Connected to SMTP server")
            server.login(sender_email, password)
            logging.info("Logged in successfully")
            server.send_message(msg)
            logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        logging.error(traceback.format_exc())

def fetch_user_leads_data():
    if not check_environment_variables():
        return

    try:
        print("Attempting database connection with parameters:")
        print(f"Host: {db_params['host']}")
        print(f"Port: {db_params['port']}")
        print(f"Database: {db_params['dbname']}")
        print(f"User: {db_params['user']}")
        
        # Establish connection
        conn = psycopg2.connect(**db_params)
        print("Database connection successful!")
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
