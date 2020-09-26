import booli_scrape as scraper
import booli_sql as bsql
import pandas as pd
from datetime import date
import numpy as np
import random

if __name__ == "__main__":

    sql = bsql.soldApartmentsDetails()
    cnxn, cursor = sql.connect()
    scrape_apartments = scraper.BooliApartments()
    scrape_apartments_granular = scraper.BooliApartmentsGranular()
    
    links = scrape_apartments.get_page_links('2020-09-18', '2020-01-01')
    print(f'Number of pages to scrape are: {len(links)}')
    
    run_time = 0

    

    for link in links[44:]:
        df_object_details_master = pd.DataFrame({'Avgift': [], 'Driftskostnad': [], 'Våning': [], 'Byggår': [], 'BRF': [], 'Link': []})

        html_lists = scrape_apartments.scrape_html_apartments(link)
        object_details_links = scrape_apartments.get_object_links_from_html_lists(html_lists)
        
        print(f'Number of objects on page are: {len(object_details_links)}')
        print('')
        print(' ------------------ ')
        print('')
        run_time += scrape_apartments.sleeper(random.randint(3,5), i = 1)

        for details_link in object_details_links:

            html_details_headers, html_details_values = scrape_apartments_granular.scrape_html_apartments_granular(details_link)
            run_time += scrape_apartments.sleeper(random.randint(3,5), i = 1)

            object_details_headers = scrape_apartments.get_objects_from_html_lists(html_details_headers)
            object_details_headers = scrape_apartments_granular.flatten_object_list(object_details_headers)        
            object_details_values = scrape_apartments.get_objects_from_html_lists(html_details_values)
            object_details_values = scrape_apartments_granular.flatten_object_list(object_details_values)
            df_object_details = scrape_apartments_granular.create_pandas_dataframe([object_details_values], object_details_headers, details_link)
            df_object_details = scrape_apartments_granular.clean_df_col_monthlyCharge_operatingCost_floorNumber(df_object_details)
            df_object_details = scrape_apartments_granular.clean_df_col_floorNumber(df_object_details)
            try:
                df_object_details_master = df_object_details_master.append(df_object_details[df_object_details.columns.intersection(df_object_details_master.columns)])
            except:
                pass

        sql.merge_details(df_object_details_master.fillna('NULL'), cnxn, cursor)
        
        print(f'Total time in seconds: {run_time}')