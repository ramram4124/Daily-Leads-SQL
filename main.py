import psycopg2
import traceback
from tabulate import tabulate
import pandas as pd
from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import sys
import matplotlib.pyplot as plt
import seaborn as sns
from email.mime.image import MIMEImage
import io

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

def create_table_image(df):
    """Convert DataFrame to a styled image"""
    # Set the style
    sns.set_style("whitegrid")
    
    # Create figure and axis with appropriate sizing
    # Increased figure size
    fig, ax = plt.subplots(figsize=(14, len(df) * 0.7 + 1))
    
    # Remove axes
    ax.set_axis_off()
    
    # Create table
    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='center',
        loc='center',
        colColours=['#f2f2f2'] * len(df.columns)
    )
    
    # Style the table
    table.auto_set_font_size(False)
    # Increased font size from 9 to 12
    table.set_fontsize(12)
    # Adjusted scale factors for better proportions
    table.scale(1.5, 1.8)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save to bytes buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='jpg', dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    
    return buf




def send_email(table_html, df):
    sender_email = os.environ.get('EMAIL_SENDER')
    # Split receiver emails by comma and strip whitespace
    receiver_emails = [email.strip() for email in os.environ.get('EMAIL_RECEIVER').split(',')]
    password = os.environ.get('EMAIL_PASSWORD')

    logging.info(f"Sending email from: {sender_email} to: {', '.join(receiver_emails)}")

    msg = MIMEMultipart()
    msg['Subject'] = f'Lead Report - {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)

    html_content = f"""
    <html>
        <body>
            <h2>Lead Report</h2>
            <p>Please find the lead report attached as an image.</p>
            <img src="cid:table_image">
        </body>
    </html>
    """
    
    msg.attach(MIMEText(html_content, 'html'))

    try:
        img_buf = create_table_image(df)
        img = MIMEImage(img_buf.read())
        img.add_header('Content-ID', '<table_image>')
        img.add_header('Content-Disposition', 'attachment', filename='lead_report.jpg')
        msg.attach(img)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())
            logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        logging.error(traceback.format_exc())
def send_email(table_html, df):
    sender_email = os.environ.get('EMAIL_SENDER')
    # Split receiver emails by comma and strip whitespace
    receiver_emails = [email.strip() for email in os.environ.get('EMAIL_RECEIVER').split(',')]
    password = os.environ.get('EMAIL_PASSWORD')

    logging.info(f"Sending email from: {sender_email} to: {', '.join(receiver_emails)}")

    msg = MIMEMultipart()
    msg['Subject'] = f'Lead Report - {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)

    html_content = f"""
    <html>
        <body>
            <h2>Lead Report</h2>
            <p>Please find the lead report attached as an image.</p>
            <img src="cid:table_image">
        </body>
    </html>
    """
    
    msg.attach(MIMEText(html_content, 'html'))

    try:
        img_buf = create_table_image(df)
        img = MIMEImage(img_buf.read())
        img.add_header('Content-ID', '<table_image>')
        img.add_header('Content-Disposition', 'attachment', filename='lead_report.jpg')
        msg.attach(img)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())
            logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        logging.error(traceback.format_exc())


def fetch_user_leads_data):
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
        send_email(html_table, df)
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    fetch_user_leads_data()

    #oqenmtwayihewaws
