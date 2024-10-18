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
            reports = reports.to_dict('records')
            if reports.empty:
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
    rows = table.find_all("tr")

    brands = []
    for r in rows:
        header = r.find("th")
        if header:
            link = header.find("a", href=True)
            if link:
                brands.append(base_url + link["href"])
     
    logger.info("Fetching PSU report links...")
    reports = []
    with alive_bar(len(brands)) as bar:
        for i in brands:
            request = session.get(i)
            try:
                table = soup.find(id="myTable")
                soup = BeautifulSoup(request.text, "html5lib")
                table = soup.find(id="myTable")
                rows = table.find_all("tr")
                brandname = rows[0].find("th").text
            except (AttributeError, IndexError):
                time.sleep(2)
                table = soup.find(id="myTable")
                soup = BeautifulSoup(request.text, "html.parser")
                table = soup.find(id="myTable")
                rows = table.find_all("tr")
                brandname = rows[0].find("th").text
            modelname = ""
            form_factor = ""
            rating = ""
            for r in rows:
                header = r.find("th")
                if header:
                    continue

                td = r.find_all("td")
                if len(td) > 8:
                    modelname = td[0].text
                    form_factor = td[1].text
                    rating = td[8].text
                else:
                    continue
                links = r.find_all("a")

                for a in links:
                    if a:
                        download = a.get("download")
                        if download:
                            if "SHORT" in a.text:
                                continue
                            link = base_url + a.get("href")
                            entry = {'Brand': brandname, 'Model': modelname, 'Form Factor': form_factor, 'Cybenetics Rating': rating, 'Report Link': link}
                            reports.append(entry)
            bar()

    reports = DataFrame.from_dict(reports)
    reports.to_csv("Reports.csv", encoding="utf-8", index=False)

    return reports


def augment_cybenetics_reports(reports):
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


def augment_geizhals_prices(reports):
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


def write_reports(data):
    print(data)
    data.to_csv("PSUs.csv", encoding='utf-8', index=False)


def main():
    reports = get_cybenetics_links()
    if reports.empty:
        return
    augment_cybenetics_reports(reports)
    reports = augment_geizhals_prices(reports)
    write_reports(reports)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
