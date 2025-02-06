import streamlit as st
import boto3
import json
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Set up the AWS session
session = boto3.Session()
# session = boto3.Session(profile_name='hackathon-participant-418272756489')
s3_client = session.client("s3")

# S3 Bucket and Folder Paths
bucket_name = 'cibcscraperresults'
folder_paths = {
    'CFTC': 'regulatory-scraped-data/cibc_CFTC_data_class/',
    'CSA': 'regulatory-scraped-data/cibc_CSA_data_class/',
    'FCA': 'regulatory-scraped-data/cibc_FCA_data_class/'
}

# Email Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "hackathoncibc@gmail.com"
SMTP_PASSWORD = "jzfa ghyn rsvi vosk"
SENDER_EMAIL = "hackathoncibc@gmail.com"
RECIPIENT_EMAIL = "hemnath283@gmail.com"
 
 
# Track last known news updates
last_news_updates = {}
 
# Function to send an email notification
def send_email_notification(subject, body):
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
 
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        server.quit()
        st.success("📩 Email notification sent successfully!")
    except Exception as e:
        st.error(f"⚠ Failed to send email: {str(e)}")

# Function to list JSON files in S3
def get_all_json_files(bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]

# Function to load JSON data from S3
def get_json_from_s3(bucket_name, file_key):
    file_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_object['Body'].read().decode('utf-8')
    return json.loads(file_content)

# Fetch and combine all data for trend analysis
def get_all_data():
    all_data = []
    for category, path in folder_paths.items():
        files = get_all_json_files(bucket_name, path)
        for file in files:
            data = get_json_from_s3(bucket_name, file)
            if isinstance(data, list):
                for record in data:
                    record["category"] = category  # Add category label
                    all_data.append(record)
    return pd.DataFrame(all_data)

# Function to display regulatory updates
def display_regulatory_updates():
    st.markdown("<h1 style='text-align: center; color: #ff4d4d;'>📢 Regulatory Updates</h1>", unsafe_allow_html=True)
    selected_folder = st.selectbox("📁 Select a Regulatory Body:", list(folder_paths.keys()))
    folder_path = folder_paths[selected_folder]
    latest_file = get_all_json_files(bucket_name, folder_path)

    if latest_file:
        latest_file = latest_file[-1]  # Select the most recent file
        json_data = get_json_from_s3(bucket_name, latest_file)

        if isinstance(json_data, list):
            df = pd.DataFrame(json_data[:5])
            if 'content' in df.columns:
                df = df.drop(columns=['content'])
 
            st.markdown(f"<h3 style='color: #4CAF50;'>🔍 Latest Update from {selected_folder}</h3>", unsafe_allow_html=True)
            st.table(df)
            global last_news_updates
            current_top_5_news = df.to_dict()
 
            if selected_folder not in last_news_updates or last_news_updates[selected_folder] != current_top_5_news:
                last_news_updates[selected_folder] = current_top_5_news
                email_body = f"New regulatory updates available for {selected_folder}:\n\n{df.to_string(index=False)}"
                send_email_notification(f"🔔 {selected_folder} News Update", email_body)
        else:
            st.json(json_data)
    else:
        st.warning(f"⚠ No JSON files found in `{selected_folder}` category.")

# Function to display trends
def display_trends():
    st.markdown("<h2 style='text-align: center; color: #0099ff;'>📈 Trends in Regulatory Updates</h2>", unsafe_allow_html=True)
    df = get_all_data()
    if df.empty:
        st.warning("⚠ No trend data available.")
        return

    # Convert date column if available
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Count occurrences per category
    category_counts = df['category'].value_counts()

    # Plot the category-wise summary
    fig, ax = plt.subplots()
    category_counts.plot(kind='bar', ax=ax, color=['#ff4d4d', '#4CAF50', '#0099ff'])
    ax.set_ylabel("Number of Updates")
    ax.set_title("Regulatory Activity by Category")
    st.pyplot(fig)

    # Generate Word Cloud for trending topics
    st.markdown("<h3 style='color: #ff8c00;'>🗞️ Trending Keywords</h3>", unsafe_allow_html=True)
    text_data = " ".join(df.get("title", "").dropna().astype(str))
    wordcloud = WordCloud(width=800, height=400, background_color="white").generate(text_data)
    fig, ax = plt.subplots()
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    # Show trends over time if 'date' is available
    if 'date' in df.columns:
        df['date'] = df['date'].dt.date
        trend_counts = df.groupby('date').size()

        fig, ax = plt.subplots()
        trend_counts.plot(ax=ax, marker='o', linestyle='-')
        ax.set_ylabel("Number of Updates")
        ax.set_title("Regulatory Updates Over Time")
        ax.grid(True)
        st.pyplot(fig)

# Function to interact with AWS Lex bot
def get_lex_response(user_input):
    lex_client = session.client('lexv2-runtime', region_name="us-west-2")
    try:
        response = lex_client.recognize_text(
            botId='3KLUNOPSAU',
            botAliasId='CWLWJXEBZX',
            localeId='en_US',
            sessionId="test_session",
            text=user_input
        )
        return response['messages'][0]['content']
    except Exception as e:
        return f"Error: {str(e)}"

# Function to display learning resources
def display_learning_resources():
    st.markdown("<h2 style='text-align: center; color: #0099ff;'>📚 Learning Resources</h2>", unsafe_allow_html=True)
 
    resources = {
        "CSA": [
            {"title": "CSA Website", "link": "https://www.securities-administrators.ca/"},
            {"title": "CSA Regulations", "link": "https://www.osc.ca/en/securities-law/instruments-rules-policies"},
        ],
        "CFTC": [
            {"title": "CFTC Official Website", "link": "https://www.cftc.gov/"},
            {"title": "CFTC Laws & Regulations", "link": "https://www.cftc.gov/LawRegulation/index.htm"},
        ],
        "FCA": [
            {"title": "FCA Official Website", "link": "https://www.fca.org.uk/"},
            {"title": "FCA Handbook", "link": "https://www.handbook.fca.org.uk/"},
        ],
    }
 
    selected_body = st.selectbox("📌 Select a Regulatory Body:", list(resources.keys()))
    st.write("🔗 **Useful Links:**")
 
    for resource in resources[selected_body]:
        st.markdown(f"- [{resource['title']}]({resource['link']})")
# Chatbot interaction
def chat_with_bot():
    st.markdown("<h1 style='text-align: center; color: #0099ff;'>💬 Lex Chatbot</h1>", unsafe_allow_html=True)
    st.write("🤖 **Ask any question related to regulatory updates**")

    freeform_text = st.text_input("📝 **Enter your question:**")
    if st.button("Ask Bot"):
        if freeform_text:
            response = get_lex_response(freeform_text)
            st.success(f"🤖 Bot: {response}")
        else:
            st.warning("⚠ Please enter a question before clicking 'Ask Bot'.")

# Sentiment and Entity Analysis with AWS Comprehend
def analyze_text_with_comprehend(text):
    comprehend_client = session.client("comprehend", region_name="us-west-2")
    
    try:
        sentiment_response = comprehend_client.detect_sentiment(Text=text, LanguageCode="en")
        sentiment = sentiment_response['Sentiment']
        
        entities_response = comprehend_client.batch_detect_entities(TextList=[text], LanguageCode="en")
        entities = entities_response['ResultList'][0]['Entities']
        entity_names = [entity['Text'] for entity in entities]
        
        return sentiment, entity_names
    except Exception as e:
        return "Error", []
    
# Function to classify language structure (Compliance Requirement or Market Update)
def classify_language_structure(text):
    # Keywords to identify compliance vs market update
    compliance_keywords = ['compliance', 'requirement', 'mandatory', 'obligation', 'rule']
    
    # Check if any of the keywords are present in the title or content
    if any(keyword in text.lower() for keyword in compliance_keywords):
        return "Compliance Requirement"
    else:
        return "Market Update"

# Sentiment and Analysis Radio Button
def sentiment_analysis_option():
    selected_folder = st.selectbox("📁 Select a Regulatory Body for Sentiment Analysis:", list(folder_paths.keys()))
    folder_path = folder_paths[selected_folder]
    latest_file = get_all_json_files(bucket_name, folder_path)

    if latest_file:
        latest_file = latest_file[-1]  # Select the most recent file
        json_data = get_json_from_s3(bucket_name, latest_file)

        if isinstance(json_data, list):
            df = pd.DataFrame(json_data[:5])  # Convert to dataframe
            # Sentiment and Entity Analysis
            if 'content' in df.columns:
                selected_text = st.selectbox("Select text for sentiment analysis", df['content'].head(5))
                sentiment, entities = analyze_text_with_comprehend(selected_text)
                st.write("Sentiment:", sentiment)
                st.write("Entities:", entities)
                 # Classify the language structure
                language_structure = classify_language_structure(selected_text)
                st.write(f"Language Structure: {language_structure}")
            else:
                st.warning("⚠ No 'content' field found in the regulatory data.")
        else:
            st.json(json_data)
    else:
        st.warning(f"⚠ No JSON files found in `{selected_folder}` category.")

# Streamlit UI
def main():
    st.sidebar.title("📌 RegCentral")
    option = st.sidebar.radio("🔍 Choose an option", ("Latest Regulatory Updates & Trends","Learning Resources", "Chat with Bot", "Sentiment & Entity Analysis"))
    
    if option == "Latest Regulatory Updates & Trends":
        display_regulatory_updates()
        display_trends()
    elif option == "Learning Resources":
        display_learning_resources()
    elif option == "Chat with Bot":
        chat_with_bot()
    elif option == "Sentiment & Entity Analysis":
        sentiment_analysis_option()

if __name__ == "__main__":
    main()
