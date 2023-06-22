import imaplib
import email
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
import psycopg2

class EmailProcessor:
    def __init__(self, username, password, db_params):
        self.username = username
        self.password = password
        self.imap = imaplib.IMAP4_SSL("imap.gmail.com")
        self.db_params = db_params
    
    def process_emails(self):
        # login
        result = self.imap.login(self.username, self.password)
        if result[0] != 'OK':
            raise Exception("Failed to log in to the email server.")
        
        # Use "[Gmail]/All Mail" for fetching all emails.
        self.imap.select('"[Gmail]/All Mail"',
                        readonly=True)
        
        response, messages = self.imap.search(None, 'UnSeen')
        for num in messages[0].split():
            _, msg_data = self.imap.fetch(num, '(RFC822)')
            raw_email = msg_data[0][1]
            email_message = email.message_from_bytes(raw_email)
            

            date = parsedate_to_datetime(email_message["Date"])
            date_str = date.strftime("%Y-%m-%d")  # Format the Date
            time_str = date.strftime("%H:%M:%S")  # Format the time

            
            # Check for the message body
            if email_message.is_multipart():
                for part in email_message.walk():
                    content_type = part.get_content_type()
                    if content_type == 'text/plain' or content_type == 'text/html':
                        message_body = part.get_payload(decode=True).decode('utf-8')
                        # print("Email Body:")
                        # print(message_body)
                        break
            else:
                message_body = email_message.get_payload(decode=True).decode('utf-8')
                # print("Message Body:")
                # print(message_body)
                soup = BeautifulSoup(message_body, 'html.parser')
                tables = soup.find_all('table')
                
                for table in tables:
                    first_names = []
                    last_names = []
                    job_ids = []
                    salaries = []
                                   
                    rows = table.find_all('tr')   # Find all rows in the table body
              
                    skip_first = True
                
                    # Loop the each row and extract the data
                    for row in rows:
                        cells = row.find_all('td')   # Find all cells in the row
                
                        if len(cells) == 4:          # Checking for the four cells in the row
                            if skip_first:           # Skip the first row in the four is equal
                                skip_first = False
                                continue
                
                            # Extract the correct data from the cells
                            first_name = cells[0].text.strip()
                            last_name = cells[1].text.strip()
                            job_id = cells[2].text.strip()
                            salary = cells[3].text.strip()
                
                            # Append the data to respective lists
                            first_names.append(first_name)
                            last_names.append(last_name)
                            job_ids.append(job_id)
                            salaries.append(salary)
                
                        elif len(cells) < 4:
                            skip_first = True
                
                    # Print the extracted data
                    for i in range(len(first_names)):
                        print("First Name:", first_names[i])
                        print("Last Name:", last_names[i])
                        print("Job ID:", job_ids[i])
                        print("Salary:", salaries[i])
                        print("Date:", date_str)
                        print("Time:", time_str)
                        print("")

                        # Upload the data into PostgreSQL
                        conn = psycopg2.connect(**self.db_params)
                        cursor = conn.cursor()

                        insert_query = """
                            INSERT INTO email_records (first_name, last_name, job_id, salary, received_Date, received_time)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """

                        data = (first_names[i], last_names[i], job_ids[i], salaries[i], date_str, time_str)
                        cursor.execute(insert_query, data)

                        conn.commit()

                        cursor.close()
                        conn.close()

                        print("Data uploaded to PostgreSQL successfully.")
                        print()

# Database Connection:
username = "cameo.task@gmail.com"
password = "imdhmhitvcmyincc"
db_params = {
    "host": "localhost",
    "port": "5432",
    "database": "EmailDB",
    "user": "postgres",
    "password": "7010028358"
}

processor = EmailProcessor(username, password, db_params)
processor.process_emails()
