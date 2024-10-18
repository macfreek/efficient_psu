import logging
import urllib
import re
import time
import os
from random import uniform
from urllib.parse import urlencode

from alive_progress import alive_bar
from bs4 import BeautifulSoup
import fitz
from pandas import DataFrame, read_csv, concat
from pandas.errors import EmptyDataError
import requests

session = requests.Session()

def randsleep():
    time.sleep(uniform(0.5, 2.5))

def get_cybenetics_links() -> DataFrame:
    logger = logging.getLogger('efficient_psu')
    if os.path.isfile("Reports.csv"):
        try:
            reports = read_csv("Reports.csv")
            if not reports.empty:
                return reports
        except EmptyDataError as err:
            logger.warning(str(err))
            pass

    base_url = "https://www.cybenetics.com/"
    url = base_url + "index.php?option=database&params=2,1,0"
    logger.info(f"Loading {url}")
    request = session.get(url)
    soup = BeautifulSoup(request.text, "html5lib")

    table = soup.find(id="myTable")
    if not table:
        logger.error(f"Could not find myTable at {url}")
        return DataFrame()
    rows = table.find_all("tr")

    brands = []
    for r in rows:
        try:
            header = r.find("th")
            link = header.find("a", href=True)
            if not link:
                continue
        except (AttributeError, ValueError) as err:
            continue
        url = base_url + link["href"]
        try:
            params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            if not params:
                continue
            brand_id = int(params['params'][0].split(',')[-1])
            brands.append(brand_id)
        except (AttributeError, ValueError, KeyError) as err:
            logger.warning(f"Could not detect brand ID from {url}: {err}")

    logger.info("Fetching PSU report links...")
    reports = []
    with alive_bar(len(brands)) as bar:
        for brand_id in brands:
            url = f'{base_url}code/db2.php?manfID={brand_id}&cert=0&bdg=&volts=2'
            request = session.get(url)
            try:
                soup = BeautifulSoup(request.text, "html5lib")
                # table = soup.find(id="myTable")
                rows = soup.find_all("tr")
                brandname = soup.find("th", class_="title").text
                logger.debug(f'Brand {brandname}: {len(rows)} rows')
            except (AttributeError, IndexError) as err:
                logger.warning(f"Could not parse table at {url}: {err}")
                break
            for index, row in enumerate(rows):
                td = row.find_all("td")
                if len(td) < 11:
                    continue
                modelname = td[0].text.strip()
                if not modelname:
                    continue
                form_factor = td[1].text.strip()
                wattage = td[2].text.strip()
                noise = td[7].text.strip()
                pwr_rating = td[8].text.strip()
                noise_rating = td[9].text.strip()
                test_date = td[10].text.strip()
                report_links = td[11].find_all('a')
                if len(report_links) != 1:
                    logger.warning(f"Found {len(report_links)} reports for {brandname} {modelname} on {url}")
                if report_links:
                    link = base_url + report_links[0]['href']
                else:
                    link = None

                entry = {
                    'Brand': brandname,
                    'Model': modelname,
                    'Form Factor': form_factor,
                    'Power': wattage,
                    'Noise (dB(A))': noise,
                    'Cybenetics Power Rating': pwr_rating,
                    'Cybenetics Noise Rating': noise_rating,
                    'Test Date': test_date,
                    'Report Link': link,
                }
                logger.debug(repr(entry))
                reports.append(entry)
            bar()
            randsleep()

    reports = DataFrame.from_dict(reports)
    reports.to_csv("Reports.csv", encoding="utf-8", index=False)

    return reports


def augment_cybenetics_reports(reports):
    df = DataFrame(columns=["Brand", "Model", "Form Factor", "Cybenetics Rating", "20W Efficiency", "40W Efficiency", "60W Efficiency", "80W Efficiency", "Report Link", "Lowest Price (Geizhals.de)"])
    print("Fetching individual PSU data...")
    with alive_bar(len(reports)) as bar:
        for psu in reports:
            response = requests.get(psu["Report Link"])
            with open('/tmp/downloaded_pdf.pdf', 'wb') as pdf_file:
                    pdf_file.write(response.content)
            with open('/tmp/downloaded_pdf.pdf', 'rb') as file:
                pdf_reader = fitz.open(file)
                for page in pdf_reader:
                    text = page.get_text()
                    
                    if not "20-80W LOAD TESTS" in text:
                        continue
                    title_count = -1
                    for line in text.splitlines():
                        title_count += 1
                        if "20-80W LOAD TESTS" in line:
                            break

                    efficiency = [re.search(r"^.*%", line).group(0) for line in text.splitlines()[title_count:] if re.search(r"^.*%", line) is not None]

                    df_new = DataFrame([{"Brand": psu['Brand'], 
                                            "Model": psu['Model'], 
                                            "Form Factor": psu['Form Factor'], 
                                            "Cybenetics Rating": psu['Cybenetics Rating'], 
                                            "20W Efficiency": efficiency[0],
                                            "40W Efficiency": efficiency[1],
                                            "60W Efficiency": efficiency[2],
                                            "80W Efficiency": efficiency[3],
                                            "Report Link": psu['Report Link'],
                                            "Lowest Price (Geizhals.de)": psu['Lowest Price (Geizhals.de)']
                                            }])
                    df = concat([df, df_new], ignore_index=True)

            open('/tmp/downloaded_pdf.pdf', 'w').close()
            bar()
    return df


def augment_geizhals_prices(reports):
    if os.path.isfile("ReportsPriced.csv"):
        reports = read_csv("ReportsPriced.csv")
        reports = reports.to_dict('records')
        if reports:
            return reports

    print("Getting prices...")
    request = session.get("https://geizhals.de/?cat=gehps")
    # Click on 'Accept cookies'
    try:
        request.text.find_element("id", "onetrust-accept-btn-handler")
        randsleep()
    except NoSuchElementException:
        pass

    randsleep()
    for psu in reports:
        if re.match(r".*(Sample|#\d+).*", psu["Model"]):
            price = None
        else:
            params = {
                    'cat': 'gehps',
                    'asuch' : psu["Brand"] + " " + psu["Model"],
                    'v': 'e',
                    'sort': 't',
                    'bl1_id': 30
                    }
            url = f"https://geizhals.de/?{urlencode(params)}"
            print(url)
            request = session.get(url)
            randsleep()
            try:
                no_results = driver.find_element(By.CLASS_NAME, "category_list__empty-list")
                price = None
            except NoSuchElementException:
                randsleep()
                soup = BeautifulSoup(request.text, "html5lib")
                price = soup.find("div", {"id": "product0"})\
                            .find("div", {"class": "cell productlist__price"})\
                            .find("span", {"class": "gh_price"}).find("span").text
            except:
                price = None

        psu["Lowest Price (Geizhals.de)"] = price

    reports = DataFrame.from_dict(reports)
    reports.to_csv("ReportsPriced.csv", encoding="utf-8", index=False)

    return reports


def write_reports(data):
    print(data)
    data.to_csv("PSUs.csv", encoding='utf-8', index=False)


def main():
    reports = get_cybenetics_links()
    if reports.empty:
        return
    # augment_cybenetics_reports(reports)
    # reports = augment_geizhals_prices(reports)
    # augment_amazon_prices(reports)
    # augment_tweakers_prices(reports)
    write_reports(reports)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
