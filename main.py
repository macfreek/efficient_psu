import logging
import urllib
import re
import time
import os
from random import uniform
from urllib.parse import urlencode

from alive_progress import alive_bar
from bs4 import BeautifulSoup
from pandas import DataFrame, read_csv, concat
from pandas.errors import EmptyDataError
import requests

_downloader = None

def randsleep():
    time.sleep(uniform(0.5, 2.5))

def roundto(num, step=5, suffix="") -> str:
    try:
        if isinstance(num, str):
            if num == '':
                return ''
            if not num[-1].isnumeric():
                if not suffix:
                    suffix = num[-1]
                num = num[:-1]
            num = float(num)
        num = step*round(num/step)
    except (ValueError, IndexError):
        return None
    return f'{num}{suffix}'


def download_url(url: str) -> BeautifulSoup:
    randsleep()
    html = _downloader.get(url).text
    soup = BeautifulSoup(html, "html5lib")
    return soup


def get_cybenetics_links() -> DataFrame:
    logger = logging.getLogger('efficient_psu')
    if os.path.isfile("Reports.csv"):
        try:
            reports = read_csv("Reports.csv")
            if not reports.empty:
                logger.info(f'Read {len(reports)} reports with {len(reports.columns)} columns from Reports.csv')
                return reports
        except EmptyDataError as err:
            logger.warning(str(err))
            pass

    base_url = "https://www.cybenetics.com/"
    url = base_url + "index.php?option=database&params=2,1,0"
    logger.info(f"Loading {url}")
    soup = download_url(url)

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
            try:
                soup = download_url(url)
            except Exception as err:
                logger.error(f"Could not load {url}: {err}")
                break
            try:
                # table = soup.find(id="myTable")
                rows = soup.find_all("tr")
                brandname = soup.find("th", class_="title").text
                logger.debug(f'Brand {brandname}: {len(rows)} rows')
            except (AttributeError, IndexError) as err:
                logger.warning(f"Could not parse table at {url}: {err}")
                break
            for row in rows:
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

    reports = DataFrame.from_dict(reports)
    reports.to_csv("Reports.csv", encoding="utf-8", index=False)
    logger.info(f'Write {len(reports)} reports with {len(reports.columns)} columns to Reports.csv')
    return reports


def augment_cybenetics_reports(reports: DataFrame):
    logger = logging.getLogger('efficient_psu')
    if os.path.isfile("ReportsEfficiency.csv"):
        try:
            reports = read_csv("ReportsEfficiency.csv")
            if not reports.empty:
                logger.info(f'Read {len(reports)} reports with {len(reports.columns)} columns from ReportsEfficiency.csv')
                return reports
        except EmptyDataError as err:
            logger.warning(str(err))
            pass

    logging.info("Fetching individual PSU data...")
    # Add empty column to all records
    EXPECTED_POWER = ('20W', '40W', '60W', '80W')
    for power in EXPECTED_POWER:
        reports[f"{power} Efficiency"] =  len(reports)*[None]
    for power in EXPECTED_POWER:
        reports[f"{power} Noise"] =  len(reports)*[None]
    reports[f"Test Volt"] =  len(reports)*[None]
    with alive_bar(len(reports)) as bar:
        for idx, psu in tqdm(reports.iterrows()):
            url = psu["Report Link"]
            if not str(url).startswith('http'):
                continue
            try:
                soup = download_url(url)
            except Exception as err:
                logger.error(f"Could not load {url}: {err}")
                return
            try:
                # table = soup.find(id="myTable")
                section = soup.find(id='supllem-1')
                header = section.find('h3').text
                if not header or not header.startswith('Light Load Tests'):
                    logging.error(f"Expected Light Load Test section in {url}. Found {header!r}")
                    continue
                tables = section.find_all('table')
                # print(url, len(tables), header)
                if not tables:
                    logging.error(f"Could not find Light Load Test table in {url}")
                    continue
                table = tables[-1]
                # table = section.find(id='nav-YimmShBtm8-20-80-230')
                rows = table.find_all("tr")
                wall_power = table.find_all("td")[-1].text
                reports.at[idx,f"Test Volt"] = roundto(wall_power, suffix="V")
            except (AttributeError, IndexError) as err:
                logger.warning(f"Could not parse table at {url}: {err}")
                return

            if len(rows) <= 2:
                logger.warning(f"Can not parse table Light Load Tests in {url}: expected 9 rows, Found {len(rows)}")
                continue
            td = rows[0].find_all("td")
            if len(td) < 9:
                logger.warning(f"Can not parse table Light Load Tests in {url}: expected 11 columns, Found {len(td)}")
                continue
            if not td[0].text.startswith('Test'):
                logger.warning(f"Expected Column 'Test' in Light Load Tests on {url}. Got {td[0].text!r}")
                continue
            if td[6].text != 'Efficiency':
                logger.warning(f"Expected Column 'Efficiency' in Light Load Tests on {url}. Got {td[6].text!r}")
                continue
            if 'Watts' not in td[5].text:
                logger.warning(f"Expected Column 'DC/AC (Watts)' in Light Load Tests on {url}. Got {td[5].text!r}")
                continue
            if len(td) > 7 and 'Noise' in td[8].text:
                noise_col = 8
            else:
                logger.warning(f"Expected Column 'PSU Noise' in Light Load Tests on {url}. Got {td[8].text!r}")
                noise_col = None


            for row in rows[1:]:
                td = row.find_all("td")
                if not td[0].attrs.get('rowspan'):
                    continue
                if len(td) < 8:
                    logger.warning(f"Can not parse table Light Load Tests in {url}: expected 11 columns, Found {len(td)}")
                    continue
                power = roundto(td[5].text, suffix='W')
                if power is None:
                    logger.error(f"Expected Power in Watts in Light Load Tests on {url}. Got {td[5].text!r}")
                    continue
                testname = td[0].text
                if testname not in (power, '1', '2', '3', '4', '5', '6', '7', '8'):
                    logger.warning(f"Light Load Tests {testname!r} on {url} measured {td[5].text} Watt.")
                    # continue
                if power not in EXPECTED_POWER:
                    logger.warning(f"Skip Light Load Tests {testname!r} on {url} with {power} Watt.")
                    continue
                efficiency = td[6].text
                if noise_col is None:
                    noise = None
                else:
                    noise = td[8].text
                reports.at[idx,f"{power} Efficiency"] = efficiency
                reports.at[idx,f"{power} Noise"] = noise
            # if idx > 20:
            #     break
        bar()

        reports.to_csv("ReportsEfficiency.csv", encoding="utf-8", index=False)
        logger.info(f'Write {len(reports)} reports with {len(reports.columns)} columns to ReportsEfficiency.csv')


def augment_geizhals_prices(reports: DataFrame):
    if os.path.isfile("ReportsPriced.csv"):
        reports = read_csv("ReportsPriced.csv")
        reports = reports.to_dict('records')
        if reports:
            return reports

    logging.info("Getting prices...")
    url = "https://geizhals.de/?cat=gehps"
    soup = download_url(url)
    # Click on 'Accept cookies'
    try:
        request.text.find_element("id", "onetrust-accept-btn-handler")
    except NoSuchElementException:
        pass

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
            soup = download_url(url)
            try:
                no_results = driver.find_element(By.CLASS_NAME, "category_list__empty-list")
                price = None
            except NoSuchElementException:
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


def write_reports(reports: DataFrame):
    logger = logging.getLogger('efficient_psu')
    reports.to_csv("PSUs.csv", encoding='utf-8', index=False)
    logger.info(f'Write {len(reports)} reports with {len(reports.columns)} columns to PSUs.csv')
    print(reports)


def main():
    reports = get_cybenetics_links()
    if reports.empty:
        return
    augment_cybenetics_reports(reports)
    # reports = augment_geizhals_prices(reports)
    # augment_amazon_prices(reports)
    # augment_tweakers_prices(reports)
    write_reports(reports)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    _downloader = requests.Session()
    main()
