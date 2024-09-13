# SWITCHFileDownloader

`SWITCHFileDownloader` is a Python class designed to automate the download of files from specified URLs to designated file paths. It uses login credentials and a source-destination mapping provided via a DataFrame to manage multiple downloads efficiently.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Set Login Credentials](#set-login-credentials)
  - [Set Source-Destination DataFrame](#set-source-destination-dataframe)
  - [Set Source and Destination Columns](#set-source-and-destination-columns)
  - [Download Files](#download-files)
- [Example](#example)
- [Requirements](#requirements)
- [Installing a Conda Environment and Pandarallel Package](#installing-a-conda-environment-and-pandarallel-package)
  - [Install Conda](#install-conda-if-not-already-installed)
  - [Create a New Conda Environment](#create-a-new-conda-environment)

## Features

- **Authentication**: Supports login with `SWITCHemail` and `SWITCHpw` for file downloads.
- **Batch Downloads**: Downloads multiple files in parallel based on source-destination pairs.
- **Robust Error Handling**: Handles HTTP request failures, file operations errors, and invalid column names in the provided DataFrame.
- **Customizable Source-Destination Mappings**: Allows users to define the source URL and destination file path via column names in a DataFrame.

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/LuMaul/SWITCHFileDownloader.git
   cd SWITCHFileDownloader
   ```

## Usage

### Set Login Credentials
The class requires login credentials for authentication. Use the `set_login()` method to provide your `SWITCHemail` and `SWITCHpw`:

```python
SWITCHFileDownloader.set_login(SWITCHemail='your_email', SWITCHpw='your_password')
```

### Set Source-Destination DataFrame
The source-destination mappings need to be provided in a DataFrame. Use the `set_SRC_DST_df()` method to load this DataFrame:

```python
df = pd.read_csv('path_to_your_csv_file.csv')
SWITCHFileDownloader.set_SRC_DST_df(df)
```

### Set Source and Destination Columns
Specify which columns in your DataFrame represent the source URL and the destination file path:

```python
SWITCHFileDownloader.set_src_dst_column_names(src_col='source_column', dst_col='destination_column')
```

### Download Files
Once everything is set up, call the `go()` method to start the download process:

```python
downloader = SWITCHFileDownloader()
downloader.go()
```

## Example

Below is a simple example of how to use `SWITCHFileDownloader`:

```python
from SWITCHFileDownloader import SWITCHFileDownloader
import pandas as pd
import os

def main():
    MAIL = 'your_email'
    PW = 'your_password'
    
    SRC_COL_NAME = 'SWITCH url'
    DST_COL_NAME = 'abs dst path'
    
    # Set login credentials
    SWITCHFileDownloader.set_login(SWITCHemail=MAIL, SWITCHpw=PW)
    
    # Set source and destination columns
    SWITCHFileDownloader.set_src_dst_column_names(src_col=SRC_COL_NAME, dst_col=DST_COL_NAME)
    
    # Initialize the downloader
    downloader = SWITCHFileDownloader()

    # Loop over files
    for source_file in ['HDR.csv', 'TEM.csv', 'IRR.csv']:
        SRC_DST_DF = pd.read_csv(os.path.join('example_data', source_file))
        downloader.set_SRC_DST_df(SRC_DST_DF)
        downloader.go()

if __name__ == "__main__":
    main()
```

## Requirements

- Python 3.x
- `pandas`
- `pandarallel`
- `requests`
- `os`
- `logging`

## Installing a Conda Environment and Pandarallel Package

### Install Conda (if not already installed)

If you don't have Conda installed, you can download and install it from [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or [Anaconda](https://docs.anaconda.com/anaconda/install/).

### Create a New Conda Environment

To create a new Conda environment, use the following command. Replace `myenv` with your desired environment name:

```bash
conda create --name myenv anaconda
```

Activate the new environment:
```bash
conda activate myenv
```
