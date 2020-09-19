import booli_scrape as scraper
import booli_sql as bsql

if __name__ == "__main__":
    scrape = scraper.BooliApartments('2020-06-03', '2020-06-01')
    sql = bsql.soldApartments()
    cnxn, cursor = sql.connect()

    links = scrape.get_page_links()
    print(f'Number of pages to scrape are: {len(links)}')
    
    run_time = 0
    for link in links:
        html_lists = scrape.scrape_html(link)
        object_links = scrape.get_object_links_from_html_lists(html_lists)
        run_time += scrape.sleeper(i = 1)

        objects = scrape.get_objects_from_html_lists(html_lists)
        objects = scrape.remove_empty_values_from_objects(objects)
        df_objects = scrape.create_pandas_dataframe(objects, object_links)
        df_objects = scrape.expand_df_cols(df_objects)
        df_objects = scrape.drop_df_cols(df_objects)
        df_objects = scrape.clean_df_col_growth(df_objects)
        df_objects = scrape.clean_df_cols_price_sice(df_objects)

        df_objects.Growth = scrape.replace_df_comma_dot(df_objects.Growth)
        
        df_objects = scrape.set_df_col_to_numeric(df_objects)
        df_objects.Growth = df_objects.Growth/100
        df_objects = scrape.set_df_col_to_date(df_objects)

        df_objects = scrape.replace_nans_with_zeros(df_objects)

        sql.merge(df_objects, cnxn, cursor)

        print(f'Total time in seconds: {run_time}')

        