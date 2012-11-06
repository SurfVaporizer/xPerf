mysql xPerformance -e "delete from measurements where insert_time <= ( CURRENT_TIMESTAMP() - INTERVAL 31 DAY)"
