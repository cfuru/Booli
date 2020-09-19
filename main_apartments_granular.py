import booli_scrape as scraper
import booli_sql as bsql
import pandas as pd

if __name__ == "__main__":

    sql = bsql.soldApartments()
    cnxn, cursor = sql.connect()
    scrape_apartments = scraper.BooliApartments()
    scrape_apartments_granular = scraper.BooliApartmentsGranular()
    
    links = scrape_apartments.get_page_links('2020-06-03', '2020-06-03')
    print(f'Number of pages to scrape are: {len(links)}')
    
    run_time = 0

    df_object_details_master = pd.DataFrame({'Avgift': [], 'Driftkostnad': [], 'Våning': [], 'Byggår': [], 'BRF': [], 'Link': []})

    for link in links:

        html_lists = scrape_apartments.scrape_html_apartments(link)
        object_details_links = scrape_apartments.get_object_links_from_html_lists(html_lists)
        
        run_time += scrape_apartments.sleeper(i = 1)

        for details_link in object_details_links:

            html_details_headers, html_details_values = scrape_apartments_granular.scrape_html_apartments_granular(details_link)
            run_time += scrape_apartments.sleeper(i = 1)

            object_details_headers = scrape_apartments.get_objects_from_html_lists(html_details_headers)
            object_details_headers = scrape_apartments_granular.flatten_object_list(object_details_headers)        
            object_details_values = scrape_apartments.get_objects_from_html_lists(html_details_values)
            object_details_values = scrape_apartments_granular.flatten_object_list(object_details_values)
            df_object_details = scrape_apartments_granular.create_pandas_dataframe([object_details_values], object_details_headers, details_link)
            df_object_details_master = df_object_details_master.append(df_object_details[df_object_details.columns.intersection(df_object_details_master.columns)])

        print(df_object_details_master)
        
        print(f'Total time in seconds: {run_time}')