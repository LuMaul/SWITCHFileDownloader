from SWITCHFileDownloader import SWITCHFileDownloader
from BFE_get_src_dst_df import BFE_get_src_dst_df
from dependencies import *



def main():

    START = dt.datetime(2024, 9, 1)

    BFE_get_src_dst_df.get_files(root='/mnt/f/bfe_analysis/raw')

    SWITCHFileDownloader.set_login(
        SWITCHemail='your_email',
        SWITCHpw='your_pw'
    )


    SWITCHFileDownloader.set_src_dst_column_names(
        src_col='SWITCH url',
        dst_col='abs dst path'
    )

    df_getter = BFE_get_src_dst_df()
    downloader = SWITCHFileDownloader()

    for boxNr in range(1,7):

        src_dst_df = df_getter.to_df(
            boxNr=boxNr, which='hdr', start=START
            )
        
        SWITCHFileDownloader.set_SRC_DST_df(src_dst_df)
        downloader.go()


        src_dst_df = df_getter.to_df(
            boxNr=boxNr, which='tem', start=START
            )
        
        SWITCHFileDownloader.set_SRC_DST_df(src_dst_df)
        downloader.go()


    src_dst_df = df_getter.to_df(
        boxNr=1, which='irr', start=START
    )
    SWITCHFileDownloader.set_SRC_DST_df(src_dst_df)
    downloader.go()


main()
