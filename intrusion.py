import pandas as pd

class MainClass:
    def data_idea(**kwargs):
        query = '''
                    SELECT protocol_type,service, "flag",logged_in, "class", "dst_bytes","src_bytes","duration"
                    FROM train_dataset
                '''
        conn = kwargs['engine'].connect()
        df = pd.read_sql(query, conn)
        conn.close()
        return df.to_dict(orient = 'records')     
    
    def class_basis(**kwargs):
        query='''select "class" :: text, count("class") :: int as "count" FROM "train_dataset" group by "class"'''
        conn = kwargs['engine'].connect()
        df = pd.read_sql(query,conn)
        conn.close()
        return df.to_dict(orient = 'records')


    def protocol_analysis(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter == None and log_in_filter==None and protocol_type_filter==None:

            query = '''select "protocol_type" :: text, count("class") :: int as "count" FROM train_dataset group by "protocol_type"'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')
        elif kwargs['filter_value'][0] and kwargs['filter_value_nr'][0] and kwargs['filter_value_nr'][1]:
            query = '''
                        SELECT "protocol_type" :: text, 
                                count("class") :: int as "count" 
                        FROM train_dataset  
                        WHERE class = '{}' and logged_in='{}' and protocol_type='{}'
                        GROUP BY "protocol_type"
                    '''.format(class_type_filter, log_in_filter,protocol_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query, conn)
            conn.close()
            return df.to_dict(orient = 'records')

    def flag_analysis_bar(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]

        if class_type_filter == None and log_in_filter==None and protocol_type_filter==None:
            query = '''select "flag" :: text, count("class") :: int as "count" FROM train_dataset group by "flag"'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')
        elif kwargs['filter_value'][0] and kwargs['filter_value_nr'][0] and kwargs['filter_value_nr'][1]:
            query = '''
                        SELECT "flag" :: text, 
                                count("class") :: int as "count" 
                        FROM train_dataset  
                        WHERE class = '{}' and logged_in='{}' and protocol_type='{}' 
                        GROUP BY "flag"
                    '''.format(class_type_filter, log_in_filter,protocol_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query, conn)
            conn.close()
            return df.to_dict(orient = 'records')
        
            
        
                   
    def top10_services(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]

        if class_type_filter == None:
            query = '''
                    SELECT "service" :: text, count("service") :: int 
                    FROM "train_dataset" 
                    GROUP BY "service" 
                    ORDER BY count("service") desc limit 10'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')
        elif  class_type_filter:
            query = '''
                    SELECT "service" :: text, count("service") :: int 
                    FROM "train_dataset" 
                    WHERE class = '{}' and logged_in='{}' and protocol_type='{}' 
                    GROUP BY "service"
                    ORDER BY count("service") desc limit 10
                    
                        '''.format(class_type_filter, log_in_filter,protocol_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query, conn)
            conn.close()
            return df.to_dict(orient = 'records')
    
    def login_analysis(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]

        if class_type_filter == None:
            query = '''select "logged_in" :: text, count("class") :: int as "count" FROM train_dataset group by "logged_in"'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')
        else:
            query = '''
                        SELECT "logged_in" :: text, 
                                count("class") :: int as "count" 
                        FROM train_dataset  
                        WHERE class = '{}' and logged_in='{}' and protocol_type='{}' 
                        GROUP BY "logged_in"
                    '''.format(class_type_filter, log_in_filter,protocol_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query, conn)
            conn.close()
            return df.to_dict(orient = 'records')
    
    def src_analysis(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''select "src_bytes" :: text, count ("src_bytes") :: int as "count" FROM "train_dataset" group by "src_bytes"
            order by count("src_bytes")'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')
        else:
            query='''select "dst_bytes" :: text, "src_bytes" :: int as "dst_value" FROM "train_dataset" 
            where class='{}'
            group by "src_bytes"
            order by count(src_bytes)'''.format(class_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
    
    def dst_analysis(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''select "dst_bytes" :: text, count ("dst_bytes") :: int as "count" FROM "train_dataset" group by "dst_bytes"
            order by count("dst_bytes")'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            df = df[df.dst_bytes != 0]
            conn.close()
            return df.to_dict(orient = 'records')
        else:
            query='''select "dst_bytes" :: text, "src_bytes" :: int as "dst_value" FROM "train_dataset" 
            where class='{}'
            group by "src_bytes"
            order by "src_bytes"'''.format(class_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
    def duration_analysis(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''select "duration" :: text as time, count ("duration") :: int as "count" FROM "train_dataset" group by "duration"
            ORDER BY count ("duration")'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')
        else:
            query='''select "dst_bytes" :: text, "src_bytes" :: int as "dst_value" FROM "train_dataset" 
            where class='{}'
            group by "src_bytes"'''.format(class_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()

    def duration_analysis_without_0(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''select "duration" :: text as time, count ("duration") :: int as "count" FROM "train_dataset" 
            where time not like '%0%'
            group by "duration"
            ORDER BY count ("duration")'''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')
        else:
            query='''select "dst_bytes" :: text as , "src_bytes" :: int as "dst_value" FROM "train_dataset" 
            where class='{}'
            group by "src_bytes"'''.format(class_type_filter)
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()    
    
    def boxplot1(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''
            select "hot"::numeric as boxvalue, "class" as boxname
            FROM "train_dataset" 
            '''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')    

    def boxplot2(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''
            select "wrong_fragment"::numeric as boxvalue, "class" as boxname
            FROM "train_dataset" 
            '''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')   

    def boxplot3(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''
            select "num_failed_logins"::numeric as boxvalue, "class" as boxname
            FROM "train_dataset" 
            '''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')   

    def interesting_analysis(**kwargs):
        return [{ 'header': 'Interesting Analysis with classes', 'body': '1)TCP has the highest count in anomalous data        2) http services is highest in the percenatge (39.5%)   3)SF is the most flagged in normal data whereas S0 is the most flagged in the anomalous data set.     4)Log in success rate drops where the class is anomalous with only 409 successful logins and 11,000 unsuccessful logins. The opposite happens when the class is normal with almost 9000 successful logins. '}]

    def boxplot4(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''
            select "duration"::numeric as boxvalue, "class" as boxname
            FROM "train_dataset" 
            '''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')   
    
    def boxplot5(**kwargs):
        class_type_filter = kwargs['filter_value'][0]
        log_in_filter=kwargs['filter_value_nr'][0]
        protocol_type_filter=kwargs['filter_value_nr'][1]
        if class_type_filter== None:
            query='''
            select "duration"::numeric as boxvalue, "protocol_type" as boxname
            FROM "train_dataset" 
            '''
            conn = kwargs['engine'].connect()
            df = pd.read_sql(query,conn)
            conn.close()
            return df.to_dict(orient = 'records')