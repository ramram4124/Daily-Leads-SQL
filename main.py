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

# Database connection parameters
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

def create_table_image(df, title):
    """Convert DataFrame to a styled image with wrapped column headers"""
    sns.set_style("whitegrid")
    
    # Function to wrap text
    def wrap_text(text, width=15):
        import textwrap
        return '\n'.join(textwrap.wrap(str(text), width=width))
    
    # Wrap column headers
    wrapped_columns = [wrap_text(col) for col in df.columns]
    
    # Calculate appropriate figure height based on content and wrapped headers
    max_header_lines = max(len(str(col).split('\n')) for col in wrapped_columns)
    fig_height = len(df) * 0.7 + (max_header_lines * 0.3) + 1
    
    # Create figure and axis with appropriate sizing
    fig, ax = plt.subplots(figsize=(14, fig_height))
    
    # Add title
    plt.title(title, pad=20, size=14, weight='bold')
    
    # Remove axes
    ax.set_axis_off()
    
    # Create table with wrapped headers
    table = ax.table(
        cellText=df.values,
        colLabels=wrapped_columns,
        cellLoc='center',
        loc='center',
        colColours=['#f2f2f2'] * len(df.columns)
    )
    
    # Style the table
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.5, 1.8)
    
    # Adjust row heights for wrapped headers
    for cell in table._cells:
        if cell[0] == 0:  # Header row
            table._cells[cell].set_height(0.15 * max_header_lines)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save to bytes buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='jpg', dpi=300, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    
    return buf

def send_email(leads_df, status_df, followups_df):
    sender_email = os.environ.get('EMAIL_SENDER')
    receiver_emails = [email.strip() for email in os.environ.get('EMAIL_RECEIVER').split(',')]
    password = os.environ.get('EMAIL_PASSWORD')

    logging.info(f"Sending email from: {sender_email} to: {', '.join(receiver_emails)}")

    msg = MIMEMultipart()
    msg['Subject'] = f'Daily Reports - {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)

    html_content = f"""
    <html>
        <body>
            <h2>Daily Reports - {datetime.now().strftime("%Y-%m-%d")}</h2>
            <h3>1. Lead Generation Report</h3>
            <img src="cid:leads_image">
            <h3>2. Status-wise Report</h3>
            <img src="cid:status_image">
            <h3>3. Follow-up Report</h3>
            <img src="cid:followups_image">
        </body>
    </html>
    """
    
    msg.attach(MIMEText(html_content, 'html'))

    try:
        # Attach leads report image
        leads_buf = create_table_image(leads_df, "Lead Generation Report")
        leads_img = MIMEImage(leads_buf.read())
        leads_img.add_header('Content-ID', '<leads_image>')
        msg.attach(leads_img)

        # Attach status report image
        status_buf = create_table_image(status_df, "Status-wise Report")
        status_img = MIMEImage(status_buf.read())
        status_img.add_header('Content-ID', '<status_image>')
        msg.attach(status_img)

        # Attach followups report image
        followups_buf = create_table_image(followups_df, "Follow-up Report")
        followups_img = MIMEImage(followups_buf.read())
        followups_img.add_header('Content-ID', '<followups_image>')
        msg.attach(followups_img)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())
            logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        logging.error(traceback.format_exc())

def fetch_user_leads_data():
    if not check_environment_variables():
        return

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Leads generation query
        leads_query = """
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

        # Status-wise query
        status_query = """
        SELECT 
            u.name as telecaller_name,
            COUNT(CASE WHEN l.status = 'Open' THEN 1 END) as open_calls,
            COUNT(CASE WHEN l.status = 'Completed' THEN 1 END) as completed_calls,
            COUNT(CASE WHEN l.status = 'Feedback' THEN 1 END) as feedback_calls,
            COUNT(CASE WHEN l.status = 'Confirmed' THEN 1 END) as confirmed_calls,
            COUNT(CASE WHEN l.status = 'Not Interested' THEN 1 END) as not_interested_calls,
            COUNT(CASE WHEN DATE(l.created_at) = CURRENT_DATE THEN 1 END) as new_leads_today,
            COUNT(CASE WHEN DATE(l.modified_at) = CURRENT_DATE AND DATE(l.created_at) != CURRENT_DATE THEN 1 END) as followup_calls,
            COUNT(CASE WHEN DATE(l.modified_at) = CURRENT_DATE THEN 1 END) as total_calls_made
        FROM 
            "user" u
            LEFT JOIN lead l ON l.creator_id = u.id
        WHERE 
            (DATE(l.created_at) = CURRENT_DATE OR DATE(l.modified_at) = CURRENT_DATE)
            AND u.is_admin = false
        GROUP BY 
            u.id, u.name
        ORDER BY 
            total_calls_made DESC;
        """
        
        # Followups query
        followups_query = """
        WITH followup_stats AS (
            SELECT 
                u.name as telecaller_name,
                COUNT(l.id) as total_followups_assigned,
                COUNT(CASE 
                    WHEN DATE(l.modified_at) = CURRENT_DATE 
                    THEN 1 
                    END) as followups_completed
            FROM 
                "user" u
                LEFT JOIN lead l ON l.creator_id = u.id
            WHERE 
                DATE(l.followup_date) = CURRENT_DATE
                AND u.is_admin = false
            GROUP BY 
                u.id, u.name
        )
        SELECT 
            TO_CHAR(CURRENT_DATE, 'DDth-Mon') as report_date,
            telecaller_name,
            total_followups_assigned,
            followups_completed,
            (total_followups_assigned - followups_completed) as pending_followups,
            ROUND((followups_completed::numeric / NULLIF(total_followups_assigned, 0) * 100)::numeric, 2) as completion_percentage
        FROM 
            followup_stats
        ORDER BY 
            completion_percentage DESC;
        """
        
        # Execute queries and create DataFrames
        cursor.execute(leads_query)
        leads_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        
        cursor.execute(status_query)
        status_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        
        cursor.execute(followups_query)
        followups_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        
        # Print results to console
        print("\nLead Generation Report:", datetime.now().strftime("%Y-%m-%d"), ":\n")
        print(tabulate(leads_df, headers='keys', tablefmt='pretty', showindex=False))
        
        print("\nStatus-wise Report:", datetime.now().strftime("%Y-%m-%d"), ":\n")
        print(tabulate(status_df, headers='keys', tablefmt='pretty', showindex=False))
        
        print("\nFollow-up Report:", datetime.now().strftime("%Y-%m-%d"), ":\n")
        print(tabulate(followups_df, headers='keys', tablefmt='pretty', showindex=False))
        
        # Send email with all reports
        send_email(leads_df, status_df, followups_df)
        
    except Exception as e:
        print(f"Error: {e}")
        logging.error(traceback.format_exc())
    
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    fetch_user_leads_data()
