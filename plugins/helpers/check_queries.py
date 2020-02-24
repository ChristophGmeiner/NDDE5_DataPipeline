class CheckQueries:
    useridcheck = ("""
                    SELECT COUNT(*) 
                    from (SELECT DISTINCT userid, level FROM songplays)
                   """)

    dimcheck = ("SELECT COUNT(*) FROM {}")
    
    songidcheck = ("""
                  SELECT COUNT(DISTINCT song_id) FROM staging_songs
                  """)
                  
    artistidcheck = ("""
                    SELECT COUNT(*) FROM 
                            	(SELECT distinct artist_id, 
                                 				 artist_name, 
                                 				 artist_location, 
                                                 artist_latitude, 
                                                 artist_longitude
                                    FROM staging_songs)
                    """) 
                    
    starttimecheck = ("""
                        SELECT COUNT(DISTINCT start_time) FROM songplays
                     """)
    