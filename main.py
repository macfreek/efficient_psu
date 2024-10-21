import logging
import urllib
import re
import time
import os
from dataclasses import dataclass
from random import uniform
from urllib.parse import urlencode
from typing import Optional, Iterable

from bs4 import BeautifulSoup
from pandas import DataFrame, read_csv, concat
from pandas.errors import EmptyDataError
import requests
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iter, total=1):
        for a in iter:
            yield a

try:
    # local module, see https://github.com/macfreek/game-db-manager/blob/main/downloader.py.
    from downloader import CachedDownloader
    _has_cached_downloader = True
except ImportError:
    _has_cached_downloader = False
_downloader = None


@dataclass
class TestResult:
    name: str  # name.
    power: str  # e.g. "20W". rounded to 5 Watt
    voltage: str  # e.g. "230V". rounded to 5 Volt
    efficiency: str  # e.g. "84.53%"
    noise: Optional[str]  # e.g. "17.4". In dB(A). May be None.


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
    if _has_cached_downloader:
        global _downloader
        html = _downloader.get_cached_url(url, ttl=10)
    else:
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
    for brand_id in tqdm(brands):
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

    reports = DataFrame.from_dict(reports)
    reports.to_csv("Reports.csv", encoding="utf-8", index=False)
    logger.info(f'Write {len(reports)} reports with {len(reports.columns)} columns to Reports.csv')
    return reports


def iter_testresults(soup: BeautifulSoup, url, logger: logging.Logger) -> Iterable[TestResult]:
    """Given an element, walk all tables in reverse order (so 230V result comes first),
    yielding all test results. Log unexpected parsing issue to the given logger.
    url is only used for logging."""
    # find all rows in all tables in this section
    rows = []
    try:
        section = soup.find(id='supllem-1')
        header = section.find('h3').text
        if not header or not header.startswith('Light Load Tests'):
            logger.error(f"Expected Light Load Test section in {url}. Found {header!r}")
            return
        tables = section.find_all('table')
        if not tables:
            logger.warning(f"Expected 2 Light Load Test tables in {url}. Found 0 tables.")
            return
        if len(tables) > 2:
            logger.warning(f"Expected 2 Light Load Test tables in {url}. Found {len(tables)} tables.")
    except (AttributeError, IndexError) as err:
        logger.warning(f"Could not parse table at {url}: {err}")
        return
    for table_idx, table in reversed(list(enumerate(tables))):
        table_idx += 1  # Use human numbering 1, 2, 3, ... instead of 0, 1, 2, ...
        rows = table.find_all("tr")
        if len(rows) <= 2:
            logger.warning(f"Skip Light Load Tests table #{table_idx} in {url}: expected 9 rows, Found {len(rows)}")
            continue
        row_iterator = iter(enumerate(rows))
        # examine header (first row)
        _, row = next(row_iterator)
        td = row.find_all("td")
        if len(td) < 8:
            logger.warning(f"Skip Light Load Tests table #{table_idx} in {url}: expected 11 columns, Found {len(td)}")
            continue
        if not td[0].text.startswith('Test'):
            logger.warning(f"Skip Light Load Tests table #{table_idx} in {url}: expected Column 'Test'. Found {td[0].text!r}")
            continue
        if td[6].text != 'Efficiency':
            logger.warning(f"Skip Light Load Tests table #{table_idx} in {url}: expected Column 'Efficiency'. Found {td[6].text!r}")
            continue
        if 'Watts' not in td[5].text:
            logger.warning(f"Skip Light Load Tests table #{table_idx} in {url}: expected Column 'DC/AC (Watts)'. Found  {td[5].text!r}")
            continue
        noise_col = None
        if len(td) < 9:
            pass
        elif 'Noise' in td[8].text:
            noise_col = 8
        elif 'PF/AC Volts' not in td[8].text:  # no need to warn about known lack of noise measurements
            logger.warning(f"Ignore noise in Light Load Tests table #{table_idx} in {url}: expected Column 'PSU Noise'. Found {td[8].text!r}")
        if 'PF/AC Volts' not in td[-1].text:
            logger.warning(f"Skip Light Load Tests table #{table_idx} in {url}: expected Column 'PF/AC Volts'. Found {td[-1].text!r}")
            continue

        # examine subsequent rows
        row_count = 0
        non_row_count = 0
        for row_idx, row in row_iterator:
            td = row.find_all("td")
            try:
                rowspan = int(td[0].attrs.get('rowspan', 1))
            except ValueError:
                rowspan = 1
            if rowspan == 1:
                non_row_count += 1
                continue
            # since it is a rowspan, we fetch the corresponding next row. Assume rowspan == 2
            _, next_row = next(row_iterator)
            if not next_row:
                logger.warning(f"Skip row {row_idx} in Light Load Tests table #{table_idx} in {url}: missing next row, despite rowspan={rowspan}")
                continue
            if len(td) < 8:
                logger.warning(f"Skip row {row_idx} in Light Load Tests table #{table_idx} in {url}: expected 11 columns, Found {len(td)}")
                non_row_count += 1
                continue
            if len(td) > 15:
                logger.warning(f"Skip row {row_idx} in Light Load Tests table #{table_idx} in {url}: expected 11 columns, Found {len(td)}")
                non_row_count += 1
                continue
            next_td = next_row.find_all("td")
            if rowspan != 2:
                logger.warning(f"Ignore row(s) {row_idx+2}-{row_idx+rowspan} in Light Load Tests table #{table_idx} in {url}: expected rowspan=2. Found rowspan={rowspan}")
            # skip more rows, if rowspan > 2
            for _ in range(rowspan-2):
                # skip more rows if needed.
                next_td = next(row_iterator)
            if len(next_td) not in (6, 7):
                logger.warning(f"Skip rows {row_idx}-{row_idx+1} in Light Load Tests table #{table_idx} in {url}: expected 6 or 7 columns in row {row_idx+1}. Found {len(next_td)}")
                non_row_count += 1
                continue
            power = roundto(td[5].text, suffix='W')
            if power is None:
                logger.error(f"Skip rows {row_idx}-{row_idx+1} in Light Load Tests table #{table_idx} in {url}: expected Power in Watts. Found {td[5].text!r}")
                continue
            testname = td[0].text
            if testname not in (power, '1', '2', '3', '4', '5', '6', '7', '8'):
                logger.warning(f"Ignore test name {testname!r} on {url}. Expect number or {power!r}. Found {td[5].text} Watt measurement.")
                # only give a warning, but use the reported power (not the name)
            efficiency = td[6].text
            if not efficiency or efficiency[-1] != '%':
                logger.warning(f"Skip rows {row_idx}-{row_idx+1} in Light Load Tests table #{table_idx} in {url}: expected efficiency percentage. Found {efficiency!r}.")
                continue
            if noise_col is None:
                noise = None
            else:
                noise = td[8].text
                if len(noise) > 20:
                    logger.warning(f"Ignore noise in Light Load Tests table #{table_idx} in {url}: expected noise in dB(A). Found {noise[:50]!r}....")
                    noise = None
            voltage = roundto(next_td[-1].text, suffix="V")
            if not voltage or voltage == '0V':
                logger.warning(f"Skip rows {row_idx}-{row_idx+1} in Light Load Tests table #{table_idx} in {url}: Expected Test Voltage. Found {next_td[-1].text!r}.")
                continue
            row_count += 1
            # print(f'row {i}: -> {TestResult(testname, power, voltage, efficiency, noise)}')
            yield TestResult(testname, power, voltage, efficiency, noise)
        if row_count == 0:
            logger.warning(f"Skip Light Load Tests table #{table_idx} in {url}: No valid test results found. Skipped {non_row_count} rows.")


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
    for idx, psu in tqdm(reports.iterrows(), total=len(reports)):
        url = psu["Report Link"]
        if not str(url).startswith('http'):
            continue
        try:
            soup = download_url(url)
        except Exception as err:
            logger.error(f"Could not load {url}: {err}")
            return
        
        voltages = set()
        missing_power = set(EXPECTED_POWER)
        for result in iter_testresults(soup, url, logger):
            if result.power not in EXPECTED_POWER:
                logger.warning(f"Skip Light Load Tests {result.name!r} on {url} with {result.power} Watt.")
                continue
            try:
                missing_power.remove(result.power)
            except KeyError:
                continue  # we already had a previous result. Ignore this one.
            voltages.add(result.voltage)
            # add the results to the database!
            reports.at[idx,f"{result.power} Efficiency"] = result.efficiency
            if result.noise:
                reports.at[idx,f"{result.power} Noise"] = result.noise
            if not missing_power:
                break  # we have all results
        reports.at[idx ,"Test Volt"] = '/'.join(sorted(list(voltages)))
        # if idx > 20:
        #     break

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
    if _has_cached_downloader:
        _downloader = CachedDownloader(delay=1.5)
    else:
        _downloader = requests.Session()
    main()
