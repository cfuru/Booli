import booli_scrape as scraper
import booli_sql as bsql
import random

if __name__ == "__main__":

    sql = bsql.soldApartments()
    cnxn, cursor = sql.connect()
    scrape_apartments = scraper.BooliApartments()
    
    links = scrape_apartments.get_page_links('2020-01-01', '2019-01-01')
    print(f'Number of pages to scrape are: {len(links)}')
    
    run_time = 0

    for link in links:
        html_lists = scrape_apartments.scrape_html_apartments(link)
        object_links = scrape_apartments.get_object_links_from_html_lists(html_lists)
        run_time += scrape_apartments.sleeper(random.randint(3,5), i = 1)

        objects = scrape_apartments.get_objects_from_html_lists(html_lists)
        objects = scrape_apartments.remove_empty_values_from_objects(objects)
        df_objects = scrape_apartments.create_pandas_dataframe(objects, object_links)
        df_objects = scrape_apartments.expand_df_cols(df_objects)
        df_objects = scrape_apartments.drop_df_cols(df_objects)
        df_objects = scrape_apartments.clean_df_col_growth(df_objects)
        df_objects = scrape_apartments.clean_df_cols_price_sice(df_objects)

        df_objects.Growth = scrape_apartments.replace_df_comma_dot(df_objects.Growth)
        
        df_objects = scrape_apartments.set_df_col_to_numeric(df_objects)
        df_objects.Growth = df_objects.Growth/100
        df_objects = scrape_apartments.set_df_col_to_date(df_objects)

        df_objects = scrape_apartments.replace_nans_with_zeros(df_objects)

        sql.merge(df_objects, cnxn, cursor)

        print(f'Total time in seconds: {run_time}')

        