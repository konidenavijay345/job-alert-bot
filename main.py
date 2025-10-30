import requests
from bs4 import BeautifulSoup
import schedule
import time
import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def send_telegram_message(message):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, data=data)
    except Exception as e:
        print(f"Error sending message: {e}")

def search_jobs():
    try:
        keywords = ["sdet", "qa engineer", "junior software engineer", "intern"]
        all_jobs = []

        for term in keywords:
            search_url = f"https://www.indeed.com/jobs?q={term}&l=india&fromage=1"
            response = requests.get(search_url)
            soup = BeautifulSoup(response.text, "html.parser")

            jobs = []
            for job in soup.find_all("a", class_="tapItem")[:5]:
                title = job.find("h2").text.strip()
                company = job.find("span", class_="companyName").text.strip() if job.find("span", class_="companyName") else "Unknown"
                link = "https://www.indeed.com" + job["href"]
                jobs.append(f"<b>{title}</b>\n{company}\n{link}")

            if jobs:
                all_jobs.append(f"📌 <b>{term.title()}</b>\n\n" + "\n\n".join(jobs))

        if all_jobs:
            send_telegram_message("🔥 <b>Daily Job Alerts</b> 🔥\n\n" + "\n\n".join(all_jobs))
        else:
            send_telegram_message("No new jobs found today 😅")

    except Exception as e:
        print(f"Error in search_jobs: {e}")

schedule.every().day.at("09:00").do(search_jobs)

if __name__ == "__main__":
    try:
        send_telegram_message("🤖 Job bot started on Render!")
        while True:
            schedule.run_pending()
            time.sleep(3600)
    except KeyboardInterrupt:
        print("Bot stopped manually.")
    except Exception as e:
        print(f"Fatal error: {e}")
