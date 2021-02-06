import booli_scrape as scraper
import booli_sql as bsql
import random
import pandas as pd
if __name__ == "__main__":

    sql = bsql.soldApartments()
    cnxn, cursor = sql.connect()
    scrape_apartments = scraper.BooliApartments()
    
    links = scrape_apartments.get_upcoming_page_links()
    print(f'Number of pages to scrape are: {len(links)}')
    
    run_time = 0

    for link in links[:5]:
        html_lists = scrape_apartments.scrape_html_apartments(link)
        object_links = scrape_apartments.get_upcoming_object_links_from_html_lists(html_lists)
        run_time += scrape_apartments.sleeper(random.randint(3,5), i = 1)

        objects = scrape_apartments.get_upcoming_objects_from_html_lists(html_lists)
        objects = scrape_apartments.remove_empty_values_from_objects(objects)
        
        apartments = []
        links = []
        for x in range(0, len(objects)):
            if len(objects[x]) == 6:
                apartments.append(objects[x])
                links.append(object_links[x])

        df_objects = scrape_apartments.create_upcoming_pandas_dataframe(apartments, links)
        df_objects = scrape_apartments.expand_df_cols(df_objects)
        df_objects = scrape_apartments.drop_df_cols(df_objects)
        df_objects = scrape_apartments.clean_df_col_growth(df_objects)
        df_objects = scrape_apartments.clean_df_cols_price_sice(df_objects)


