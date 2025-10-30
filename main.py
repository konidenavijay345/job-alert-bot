#!/usr/bin/env python3
import os
import sys
import json
import base64
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import smtplib
from email.message import EmailMessage
import redis
import time

# -------- Config from env -------
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
REDIS_URL = os.getenv("REDIS_URL")
MIN_RATING = float(os.getenv("MIN_RATING", "3"))
GSHEET_CREDENTIALS_JSON = os.getenv("GSHEET_CREDENTIALS_JSON")  # base64 encoded JSON
GSHEET_ID = os.getenv("GSHEET_ID")

ROLES = ["SQA","SDET","SDE","Java Developer","Software Engineer"]
SITES = ["linkedin","indeed","naukri"]

# -------- Dedupe layer (Redis or file-based) -------
class Dedupe:
    def __init__(self):
        self.use_redis = False
        self.redis_client = None
        if REDIS_URL:
            try:
                self.redis_client = redis.from_url(REDIS_URL, decode_responses=True)
                # test connection
                self.redis_client.ping()
                self.use_redis = True
                print("Using Redis for dedupe.")
            except Exception as e:
                print("Redis connection failed, using file-based dedupe:", e)
        if not self.use_redis:
            self.seen_file = "seen_jobs.txt"
            if not os.path.exists(self.seen_file):
                open(self.seen_file, "w").close()
            with open(self.seen_file, "r") as f:
                self.seen = set([line.strip() for line in f if line.strip()])

    def seen_before(self, key):
        if self.use_redis:
            # SETNX pattern: set key only if not exists
            added = self.redis_client.setnx(f"job:{key}", 1)
            if added:
                # set expiry 10 days
                self.redis_client.expire(f"job:{key}", 10 * 24 * 3600)
                return False
            return True
        else:
            if key in self.seen:
                return True
            self.seen.add(key)
            with open(self.seen_file, "a") as f:
                f.write(key + "\n")
            return False

deduper = Dedupe()

# -------- Google Sheets logging (optional) -------
gspread_client = None
if GSHEET_CREDENTIALS_JSON and GSHEET_ID:
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        cred_json = base64.b64decode(GSHEET_CREDENTIALS_JSON.encode()).decode()
        creds_obj = json.loads(cred_json)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(creds_obj, scopes=scopes)
        gspread_client = gspread.authorize(creds)
        sheet = gspread_client.open_by_key(GSHEET_ID).sheet1
        print("Google Sheets logging enabled.")
    except Exception as e:
        print("Google Sheets setup failed:", e)
        gspread_client = None

# -------- Helpers -------
def send_email(subject, body):
    if not EMAIL_USER or not EMAIL_PASSWORD:
        print("EMAIL_USER or EMAIL_PASSWORD not set. Email suppressed.")
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_USER
    msg.set_content(body)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        print("Email sent.")
        return True
    except Exception as e:
        print("Failed to send email:", e)
        return False

def write_to_sheet(row):
    if gspread_client:
        try:
            sheet.append_row(row)
        except Exception as e:
            print("Failed to append to Google Sheet:", e)

def urgency_label(rating):
    if rating is None:
        return "⚠️ No rating"
    try:
        r = float(rating)
        if r >= 4.6:
            return "🔥 APPLY NOW"
        if r >= 4.2:
            return "✅ Good Fit"
        if r >= 3.8:
            return "🙂 Consider"
        return "⚠️ Low"
    except:
        return "⚠️ No rating"

# -------- Site scrapers (best-effort) -------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}

def parse_indeed():
    found = []
    q = "+".join(role.replace(" ", "+") for role in ROLES)
    url = f"https://www.indeed.com/jobs?q={q}&l=India&fromage=1"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for job in soup.find_all("a", class_="tapItem"):
            try:
                title = job.find("h2").text.strip()
            except:
                title = job.get_text().strip()
            comp = job.find("span", class_="companyName")
            company = comp.text.strip() if comp else "Unknown"
            rating_tag = job.find("span", class_="ratingNumber")
            rating = rating_tag.text.strip() if rating_tag else None
            link = job.get("href")
            if link and link.startswith("/rc/"):
                link = "https://www.indeed.com" + link
            elif link and link.startswith("/"):
                link = "https://www.indeed.com" + link
            key = f"indeed::{link}"
            # filter rating
            try:
                if rating:
                    if float(rating) < MIN_RATING:
                        continue
            except:
                pass
            if not deduper.seen_before(key):
                found.append({
                    "site":"Indeed",
                    "title":title,
                    "company":company,
                    "rating":rating,
                    "link":link
                })
    except Exception as e:
        print("Indeed scraping failed:", e)
    return found

def parse_naukri():
    found = []
    # Naukri blocks heavy scraping. We do best-effort.
    q = "%20OR%20".join([role.replace(" ", "%20") for role in ROLES])
    url = f"https://www.naukri.com/{q}-jobs-in-india?f_TPR=r86400"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for job in soup.find_all("article", {"class":"jobTuple"}):
            try:
                t = job.find("a", {"class":"title"})
                title = t.text.strip() if t else job.get_text().strip()
                company_tag = job.find("a", {"class":"subTitle"})
                company = company_tag.text.strip() if company_tag else "Unknown"
                link = t.get('href') if t else None
                # Naukri doesn't always show rating. skip rating filter if none
                rating = None
                key = f"naukri::{link}"
                if rating:
                    try:
                        if float(rating) < MIN_RATING:
                            continue
                    except:
                        pass
                if link and not deduper.seen_before(key):
                    found.append({
                        "site":"Naukri",
                        "title":title,
                        "company":company,
                        "rating":rating,
                        "link":link
                    })
    except Exception as e:
        print("Naukri scraping failed:", e)
    return found

def parse_linkedin():
    found = []
    # LinkedIn is strict about scraping; this tries a public job-search page for last 24 hours
    q = "%20".join([role for role in ROLES])
    url = f"https://www.linkedin.com/jobs/search/?keywords={requests.utils.quote(q)}&location=India&f_TPR=r86400"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "lxml")
        # LinkedIn's markup varies; we search for job cards
        cards = soup.find_all("a", {"class":"result-card__full-card-link"})
        if not cards:
            cards = soup.find_all("a", {"class":"base-card__full-link"})
        for job in cards:
            title = job.find("h3")
            title = title.text.strip() if title else job.text.strip()
            # company often in adjacent element
            parent = job.parent
            comp = None
            rating = None
            link = job.get("href")
            company_tag = parent.find("h4")
            if company_tag:
                comp = company_tag.text.strip()
            key = f"linkedin::{link}"
            # LinkedIn does not provide ratings in listing page
            if link and not deduper.seen_before(key):
                found.append({
                    "site":"LinkedIn",
                    "title":title,
                    "company":comp or "Unknown",
                    "rating":rating,
                    "link":link
                })
    except Exception as e:
        print("LinkedIn scraping failed:", e)
    return found

# -------- Main orchestration -------
def run_all():
    results = []
    # each site
    results += parse_indeed()
    results += parse_naukri()
    results += parse_linkedin()

    if not results:
        print("No new jobs found.")
        return

    # Build email body
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"New job matches — {now}\nMinimum rating: {MIN_RATING}\n"]
    for item in results:
        rating = item.get("rating")
        label = urgency_label(rating)
        lines.append(f"{label} — {item['title']} @ {item['company']} (Site: {item['site']})")
        lines.append(item['link'] or "No link")
        lines.append(f"Rating: {rating}")
        lines.append("-" * 60)

        # Google sheet write (date, title, company, rating, link, urgency)
        try:
            write_to_sheet([now, item['title'], item['company'], rating or "", item['link'] or "", label])
        except Exception:
            pass

    body = "\n".join(lines)
    subject = f"[Jobs] {len(results)} new job(s) ({datetime.now().date()})"
    send_email(subject, body)
    print(body)

if __name__ == "__main__":
    run_all()
