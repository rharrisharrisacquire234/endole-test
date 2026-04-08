import os
import asyncio
import time
import re
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


def convert_value(value):
    """
    Converts Endole financial values to plain integers.
    - 'Unreported' or 'N/A' -> 0
    - '£36.84M'             -> 36840000
    - '£498.42K'            -> 498420
    - '-£1.14M'             -> -1140000
    - '£16.24B'             -> 16240000000
    - Plain numbers         -> as integer
    """
    if not value or value.strip().lower() in ("unreported", "n/a", ""):
        return 0

    value = value.strip()

    # Check for negative
    is_negative = value.startswith("-")

    # Remove £, -, + signs
    cleaned = re.sub(r"[£\-\+]", "", value).strip()

    # Extract numeric part and suffix
    match = re.match(r"^([\d,]+\.?\d*)([KMBkmb]?)$", cleaned)
    if not match:
        return value  # Return as-is if format is unrecognised

    number = float(match.group(1).replace(",", ""))
    suffix = match.group(2).upper()

    multipliers = {
        "K": 1_000,
        "M": 1_000_000,
        "B": 1_000_000_000,
        "":  1
    }

    result = int(number * multipliers.get(suffix, 1))
    return -result if is_negative else result


async def scrape_company_data(page, reg_number, company_slug):
    url = f"https://app.endole.co.uk/company/{reg_number}/{company_slug}"
    print(f"🔗 Visiting: {url}")

    await page.goto(url)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(5000)

    turnover, employees = "N/A", "N/A"
    total_assets, total_liabilities, net_assets = "N/A", "N/A", "N/A"

    try:
        fin_frame = next((f for f in page.frames if "tile=financials" in f.url), None)

        if fin_frame:
            await fin_frame.wait_for_load_state("domcontentloaded")

            fields = {
                "Turnover": None,
                "Employees": None,
                "Total Assets": None,
                "Total Liabilities": None,
                "Net Assets": None,
            }

            # Get all items in one pass
            items = fin_frame.locator("div.item")
            count = await items.count()

            for i in range(count):
                item = items.nth(i)
                label_el = item.locator("div.heading.-size-s")
                value_el = item.locator("div.heading.-size-l")

                if await label_el.count() > 0 and await value_el.count() > 0:
                    label = (await label_el.first.text_content() or "").strip()
                    value = (await value_el.first.text_content() or "").strip()

                    if label in fields:
                        fields[label] = value

            turnover = fields["Turnover"] or "N/A"
            employees = fields["Employees"] or "N/A"
            total_assets = fields["Total Assets"] or "N/A"
            total_liabilities = fields["Total Liabilities"] or "N/A"
            net_assets = fields["Net Assets"] or "N/A"

        else:
            print("⚠️ Financials iframe not found")

    except Exception as e:
        print(f"⚠️ Error scraping financials: {e}")

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

                # Convert financial values to plain integers
                turnover_converted       = convert_value(turnover)
                total_assets_converted   = convert_value(total_assets)
                total_liab_converted     = convert_value(total_liabilities)
                net_assets_converted     = convert_value(net_assets)

                # Employee Size: keep as-is (already a plain number), just handle N/A
                emp_converted = 0 if emp_size in ("N/A", "", None) else emp_size

                updates.append({"range": f"{chr(65 + turnover_idx)}{row_number}",          "values": [[turnover_converted]]})
                updates.append({"range": f"{chr(65 + employee_idx)}{row_number}",          "values": [[emp_converted]]})
                updates.append({"range": f"{chr(65 + total_assets_idx)}{row_number}",      "values": [[total_assets_converted]]})
                updates.append({"range": f"{chr(65 + total_liabilities_idx)}{row_number}", "values": [[total_liab_converted]]})
                updates.append({"range": f"{chr(65 + net_assets_idx)}{row_number}",        "values": [[net_assets_converted]]})

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
