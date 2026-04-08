import os
import asyncio
import time
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# Load environment variables
load_dotenv()
EMAIL = os.getenv("ENDOLE_EMAIL")
PASSWORD = os.getenv("ENDOLE_PASSWORD")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Authenticate Google Sheets
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet("Company")

# Get full sheet data
all_values = sheet.get_all_values()
headers = all_values[0]
rows = all_values[1:]

# Ensure required columns exist
for col in ["Turnover", "Employee Size", "Total Assets", "Total Liabilities", "Net Assets"]:
    if col not in headers:
        headers.append(col)
        for row in rows:
            row.append("")

sheet.update(values=[headers], range_name="A1")

# Get column indexes
reg_num_idx = headers.index("Companies House Regestration Number")
reg_name_idx = headers.index("Companies House Regestration Name")
turnover_idx = headers.index("Turnover")
employee_idx = headers.index("Employee Size")
total_assets_idx = headers.index("Total Assets")
total_liabilities_idx = headers.index("Total Liabilities")
net_assets_idx = headers.index("Net Assets")


def create_endole_slug(company_name):
    return (
        company_name.strip()
        .lower()
        .replace("&", "and")
        .replace(",", "")
        .replace(".", "")
        .replace("'", "")
        .replace("'", "")
        .replace(" ", "-")
    )


async def scrape_company_data(page, reg_number, company_slug):
    url = f"https://app.endole.co.uk/company/{reg_number}/{company_slug}"
    print(f"🔗 Visiting: {url}")

    await page.goto(url)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(5000)

    turnover, employees = "N/A", "N/A"
    total_assets, total_liabilities, net_assets = "N/A", "N/A", "N/A"

    # --- Scrape Turnover & Employees from financials iframe ---
    try:
        fin_frame = next((f for f in page.frames if "tile=financials" in f.url), None)

        if fin_frame:
            t_elem = fin_frame.locator("//div[contains(text(),'Turnover')]/following-sibling::div")
            if await t_elem.count() > 0:
                turnover = (await t_elem.first.text_content() or "").strip()

            e_elem = fin_frame.locator("//div[contains(text(),'Employees')]/following-sibling::div")
            if await e_elem.count() > 0:
                employees = (await e_elem.first.text_content() or "").strip()

    except Exception as e:
        print(f"⚠️ Error scraping turnover/employees: {e}")

    # --- Scrape Total Assets, Total Liabilities, Net Assets from financials iframe ---
    try:
        fin_frame = next((f for f in page.frames if "tile=financials" in f.url), None)

        if fin_frame:
            # Wait for financial figures to load
            await fin_frame.wait_for_load_state("domcontentloaded")

            ta_elem = fin_frame.locator("//div[contains(text(),'Total Assets')]/following-sibling::div")
            if await ta_elem.count() > 0:
                total_assets = (await ta_elem.first.text_content() or "").strip()

            tl_elem = fin_frame.locator("//div[contains(text(),'Total Liabilities')]/following-sibling::div")
            if await tl_elem.count() > 0:
                total_liabilities = (await tl_elem.first.text_content() or "").strip()

            na_elem = fin_frame.locator("//div[contains(text(),'Net Assets')]/following-sibling::div")
            if await na_elem.count() > 0:
                net_assets = (await na_elem.first.text_content() or "").strip()

    except Exception as e:
        print(f"⚠️ Error scraping assets/liabilities: {e}")

    # --- Fallback: scrape from main page if iframe didn't return values ---
    if total_assets == "N/A" or total_liabilities == "N/A" or net_assets == "N/A":
        try:
            ta_elem = page.locator("//p[contains(text(),'Total Assets')]/following-sibling::p | //div[contains(text(),'Total Assets')]/following-sibling::div[contains(@class,'value')]")
            if await ta_elem.count() > 0:
                total_assets = (await ta_elem.first.text_content() or "").strip()

            tl_elem = page.locator("//p[contains(text(),'Total Liabilities')]/following-sibling::p | //div[contains(text(),'Total Liabilities')]/following-sibling::div[contains(@class,'value')]")
            if await tl_elem.count() > 0:
                total_liabilities = (await tl_elem.first.text_content() or "").strip()

            na_elem = page.locator("//p[contains(text(),'Net Assets')]/following-sibling::p | //div[contains(text(),'Net Assets')]/following-sibling::div[contains(@class,'value')]")
            if await na_elem.count() > 0:
                net_assets = (await na_elem.first.text_content() or "").strip()

        except Exception as e:
            print(f"⚠️ Fallback scrape error: {e}")

    print(f"✅ Scraped → Turnover: {turnover}, Employees: {employees}, Total Assets: {total_assets}, Total Liabilities: {total_liabilities}, Net Assets: {net_assets}")
    return turnover, employees, total_assets, total_liabilities, net_assets


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Login to Endole
        print("🔐 Logging in to Endole...")
        await page.goto("https://app.endole.co.uk/login")
        await page.fill("input[name='email']", EMAIL)
        await page.fill("input[name='password']", PASSWORD)
        await page.click("button[type='submit']")
        await page.wait_for_load_state("networkidle")

        print("✅ Logged in successfully.\n")

        updates = []
        batch_size = 20

        for idx, row in enumerate(rows):

            try:
                reg_number = row[reg_num_idx].strip()
                reg_name = row[reg_name_idx].strip()
                turnover_val = row[turnover_idx].strip() if row[turnover_idx] else ""
                employee_val = row[employee_idx].strip() if row[employee_idx] else ""
                total_assets_val = row[total_assets_idx].strip() if row[total_assets_idx] else ""
                total_liabilities_val = row[total_liabilities_idx].strip() if row[total_liabilities_idx] else ""
                net_assets_val = row[net_assets_idx].strip() if row[net_assets_idx] else ""

                if not reg_number or not reg_name or reg_number.lower() == "nan":
                    print(f"⏭️ Skipping invalid row {idx + 2}")
                    continue

                # Skip only if ALL fields are already populated
                if turnover_val and employee_val and total_assets_val and total_liabilities_val and net_assets_val:
                    print(f"⏭️ Skipping row {idx + 2}, already has all data")
                    continue

                slug = create_endole_slug(reg_name)
                turnover, emp_size, total_assets, total_liabilities, net_assets = await scrape_company_data(page, reg_number, slug)

                row_number = idx + 2

                updates.append({"range": f"{chr(65 + turnover_idx)}{row_number}", "values": [[turnover]]})
                updates.append({"range": f"{chr(65 + employee_idx)}{row_number}", "values": [[emp_size]]})
                updates.append({"range": f"{chr(65 + total_assets_idx)}{row_number}", "values": [[total_assets]]})
                updates.append({"range": f"{chr(65 + total_liabilities_idx)}{row_number}", "values": [[total_liabilities]]})
                updates.append({"range": f"{chr(65 + net_assets_idx)}{row_number}", "values": [[net_assets]]})

                print(f"📝 Queued update for row {row_number}")

                # Close Endole tab
                try:
                    close_btn = page.locator("div._close")
                    if await close_btn.count() > 0:
                        await close_btn.first.click()
                        await page.wait_for_timeout(1000)
                except Exception:
                    pass

                # Send batch update
                if len(updates) >= batch_size:
                    print("🚀 Sending batch update to Google Sheets...")
                    sheet.batch_update(updates)
                    updates.clear()
                    time.sleep(3)

            except Exception as e:
                print(f"❌ Error at row {idx + 2}: {e}")

        # Final batch update
        if updates:
            print("🚀 Sending final batch update...")
            sheet.batch_update(updates)

        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
