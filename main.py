from SWITCHFileDownloader import SWITCHFileDownloader
from dependencies import os, pd



def main():

    MAIL = 'your_email'
    PW = 'your_password'
    
    SRC_COL_NAME = 'SWITCH url'
    DST_COL_NAME = 'abs dst path'
    
    
    SWITCHFileDownloader.set_login(
        SWITCHemail=MAIL,
        SWITCHpw=PW
    )

    SWITCHFileDownloader.set_src_dst_column_names(
        src_col=SRC_COL_NAME,
        dst_col=DST_COL_NAME
    )

    downloader = SWITCHFileDownloader()

    for source_file in ['HDR.csv', 'TEM.csv', 'IRR.csv']:

        SRC_DST_DF = pd.read_csv(os.path.join('example_data', source_file))
        downloader.set_SRC_DST_df(SRC_DST_DF)
        downloader.go()

    
main()
