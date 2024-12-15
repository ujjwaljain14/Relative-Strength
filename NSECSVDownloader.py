import csv
import os
import requests
from urllib.parse import urljoin
import pandas as pd
from datetime import datetime, timedelta
import time

class NSECSVDownloader:
    def __init__(self):
        self.headers = {
            "Host": "www.nseindia.com",
            "Referer": "https://www.nseindia.com/get-quotes/equity?symbol=TATASTEEL",
            "X-Requested-With": "XMLHttpRequest",
            "pragma": "no-cache",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
        self.base_url = "https://www.nseindia.com"
        self.ssl_verify = True
        self.s = requests.Session()
        self.s.headers.update(self.headers)

    def _initialize_session(self):
        """Initializes the session by visiting an equity quote page."""
        equity_quote_page = "/get-quotes/equity"
        url = urljoin(self.base_url, equity_quote_page)
        self.s.get(url, verify=self.ssl_verify)

    def download_csv(self, symbol, from_date, to_date):
        """
        Downloads a CSV file of historical stock data from the NSE API and saves it locally.

        Parameters:
            symbol (str): The stock symbol (e.g., 'TATASTEEL').
            from_date (str): The start date in 'dd-mm-yyyy' format.
            to_date (str): The end date in 'dd-mm-yyyy' format.
            save_path (str): The local file path to save the CSV file.
        """
        save_path = f'temporary/{symbol}.csv'
        if "nseappid" not in self.s.cookies:
            self._initialize_session()

        # Build the API URL
        api_path = "/api/historical/cm/equity"
        params = {
            "symbol": symbol,
            "series": "[\"EQ\"]",
            "from": from_date,
            "to": to_date,
            "csv": "true",
        }
        url = urljoin(self.base_url, api_path)

        # Request the CSV file
        response = self.s.get(url, params=params, verify=self.ssl_verify)
        if response.status_code == 200:
            with open(save_path, "wb") as file:
                file.write(response.content)
            print(f"CSV file downloaded successfully and saved to {save_path}")
            return save_path
        else:
            print(f"Failed to download CSV. HTTP Status Code: {response.status_code}")
            return None

    def process_and_calculate_sma_ema(self, symbol, from_date, to_date, period=14):
        # Download the CSV file
        file_path = self.download_csv(symbol, from_date, to_date)

        if file_path:
            data = pd.read_csv(file_path)

            # Remove extra spaces from column names
            data.columns = data.columns.str.strip()

            # Clean and convert numeric columns
            numeric_columns = ['OPEN', 'HIGH', 'LOW', 'PREV. CLOSE', 'ltp', 'close', 'vwap']
            for col in numeric_columns:
                if col in data.columns:
                    # Handle both string and numeric data
                    data[col] = data[col].apply(
                        lambda x: float(str(x).replace(',', '').strip()) if isinstance(x, str) else x)

            # Check if we have enough data to calculate SMA/EMA for the given period
            if len(data) < period:
                print(f"Not enough data to calculate {period}-day SMA/EMA. Only {len(data)} data points available.")
                # Optionally return None or any custom message or value indicating insufficient data
                os.remove(file_path)  # Clean up the downloaded file
                return None

            # Find the most recent price
            recent_close_price = data.iloc[0]['close']

            # Reverse the dataset for calculations
            data = data[::-1].reset_index(drop=True)

            # Calculate SMA and EMA
            data['SMA'] = data['close'].rolling(window=period).mean()
            data['EMA'] = data['close'].ewm(span=period, adjust=False).mean()

            # Retrieve the SMA and EMA for the most recent date
            last_row = data.iloc[-1]
            sma_value = last_row['SMA']
            ema_value = last_row['EMA']

            # Delete the CSV file after processing
            os.remove(file_path)
            print(f"CSV file {file_path} deleted after processing.")
            print(f'SMA={sma_value}, EMA={ema_value}, Recent Close Price={recent_close_price}')

            return {
                "sma": sma_value,
                "ema": ema_value,
                "current_price": recent_close_price
            }
        else:
            print("Failed to process data. No CSV file found.")
            return None

    import os

    def create_ratio_csv(self, file_name, file_path, period=14):
        """
        Read a CSV file containing stock details, fetch stock data, and calculate SMA and EMA ratios.

        :param file_name: Input CSV file name (e.g., 'nifty50.csv').
        :param file_path: Directory to save the output file.
        :param period: The number of days to use for SMA and EMA (default is 14).
        """
        # Calculate date range
        to_date = datetime.today()
        from_date = to_date - timedelta(days=366)

        to_date = to_date.strftime('%d-%m-%Y')
        from_date = from_date.strftime('%d-%m-%Y')

        input_csv = f'data/{file_name}'
        output_csv = os.path.join(file_path, f'{file_name.split(".")[0]}_{period}period.csv')

        # Ensure the folder exists
        os.makedirs(file_path, exist_ok=True)

        rows = []

        # Open the input CSV file
        with open(input_csv, mode='r') as infile:
            reader = csv.DictReader(infile)

            for row in reader:
                company_name = row['Company Name']
                industry = row['Industry']
                symbol = row['Symbol']

                # Fetch data and calculate SMA/EMA
                result = self.process_and_calculate_sma_ema(symbol, from_date, to_date, period)

                if result:
                    smaratio = result['current_price'] / result['sma'] if result['sma'] else None
                    emaratio = result['current_price'] / result['ema'] if result['ema'] else None

                    rows.append({
                        'Company Name': company_name,
                        'Industry': industry,
                        'Symbol': symbol,
                        'SMA': result['sma'],
                        'EMA': result['ema'],
                        'Current Price': result['current_price'],
                        'SMARatio': smaratio,
                        'EMARatio': emaratio
                    })

        # Write results to an output CSV
        sorted_rows = sorted(rows, key=lambda x: (x['SMARatio'] is None, x['SMARatio']), reverse=True)

        # Ensure the folder exists before writing

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

        with open(output_csv, mode='w', newline='') as outfile:
            fieldnames = ['Company Name', 'Industry', 'Symbol', 'SMA', 'EMA', 'Current Price', 'SMARatio', 'EMARatio']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(sorted_rows)
        print(f"CSV file '{output_csv}' created successfully.")
        return output_csv

    def create_industry_strength_file(self, file_name, file_path, period=14):
        """
        Create a file showing the average SMARatio and EMARatio for each industry,
        sorted by average SMARatio in descending order. Also include the frequency
        of industries in the top 20 entries based on SMARatio.

        Parameters:
            :param file_name: Input CSV file name (e.g., 'nifty50.csv').
            :param period: The number of days to use for SMA and EMA (default is 14).
        """
        try:

            input_csv = self.create_ratio_csv(file_name, file_path, period)
            output_txt = f'{file_path}/Industrystrength_{file_name}_{period}period.txt'

            # Read the input CSV file
            data = pd.read_csv(input_csv)

            # ==============================
            # Report 1: Average Ratios
            # ==============================
            # Group by Industry and calculate the average SMARatio and EMARatio
            industry_avg = data.groupby('Industry').agg({
                'SMARatio': 'mean',
                'EMARatio': 'mean'
            }).reset_index()

            # Sort industries by average SMARatio in descending order
            industry_avg = industry_avg.sort_values(by='SMARatio', ascending=False)

            # ==============================
            # Report 2: Frequency in Top 20
            # ==============================
            # Sort the data by SMARatio in descending order
            sorted_data = data.sort_values(by='SMARatio', ascending=False)

            # Select the top 20 entries
            top_20 = sorted_data.head(20)

            # Count the frequency of each industry in the top 20
            industry_frequency = top_20['Industry'].value_counts().reset_index()
            industry_frequency.columns = ['Industry', 'Frequency']

            # ==============================
            # Write the Reports to File
            # ==============================
            with open(output_txt, 'w') as file:
                # Report 1: Average Ratios
                file.write("Industry Strength Report (Based on Average SMARatio and EMARatio)\n")
                file.write("=" * 80 + "\n")
                file.write(f"{'Industry':<30} {'Avg SMARatio':<15} {'Avg EMARatio':<15}\n")
                file.write("=" * 80 + "\n")
                for _, row in industry_avg.iterrows():
                    industry = row['Industry']
                    avg_smaratio = row['SMARatio']
                    avg_emaratio = row['EMARatio']
                    file.write(f"{industry:<30} {avg_smaratio:<15.4f} {avg_emaratio:<15.4f}\n")

                file.write("\n\n")

                # Report 2: Frequency in Top 20
                file.write("Industry Strength Based on Frequency in Top 20 (SMARatio)\n")
                file.write("=" * 80 + "\n")
                file.write(f"{'Industry':<30} {'Frequency':<10}\n")
                file.write("=" * 80 + "\n")
                for _, row in industry_frequency.iterrows():
                    industry = row['Industry']
                    frequency = row['Frequency']
                    file.write(f"{industry:<30} {frequency:<10}\n")

            print(f"Industry strength file '{output_txt}' created successfully.")
        except Exception as e:
            print(f"An error occurred: {e}")


# Example usage
# downloader = NSECSVDownloader()
# downloader.download_csv(
#     symbol="TATASTEEL",
#     from_date="15-11-2024",
#     to_date="15-12-2024",
# )

# downloader.process_and_calculate_sma_ema(symbol="HCLTECH", from_date="15-12-2023", to_date="15-12-2024", period=50)
# downloader.process_and_calculate_sma_ema(symbol="TATASTEEL", from_date="15-12-2023", to_date="15-12-2024", period=50)

# downloader.create_ratio_csv(file_name="nifty50.csv", period=131)
# downloader.create_industry_strength_file(file_name="nifty50.csv", period=131)
