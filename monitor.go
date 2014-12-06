package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func clear_alarms(db *sql.DB) error {
	query := `
REPLACE INTO alarms (ip, dtStart, dtEnd)
SELECT ip,
       dtStart,
       dt AS dtEnd
FROM ( SELECT ip,
              dtStart
       FROM alarms
       WHERE dtEnd IS NULL
       AND hold = 0
     ) a
LEFT JOIN (SELECT host,
                  dt
           FROM pingdata
           ORDER BY dt ASC
          ) b
ON a.ip = b.host
AND b.dt > a.dtStart
WHERE b.dt IS NOT NULL
GROUP BY a.ip
`
	fmt.Println(query)
	_, err := db.Exec(query)
	return err
}

func create_alarms(db *sql.DB) error {
	query := `
REPLACE INTO alarms (ip, dtStart) 
SELECT host_list.ip,
       start_times.dt AS dtStart
FROM (
       SELECT ip
       FROM hosts
       WHERE primary_alias = 1
     ) host_list
LEFT JOIN (
            SELECT a.host, dt FROM (SELECT host, dt FROM pingdata ORDER BY dt DESC) a GROUP BY a.host
          ) start_times
ON host_list.ip = start_times.host
LEFT JOIN (
            SELECT a.ip, a.hold FROM (SELECT ip, hold FROM alarms ORDER BY dtStart DESC) a GROUP BY a.ip
          ) existing_alarms
ON host_list.ip = existing_alarms.ip
WHERE (start_times.dt < NOW() - INTERVAL 2 MINUTE
    OR start_times.dt IS NULL)
AND existing_alarms.hold != b'1'
`
	fmt.Println(query)
	_, err := db.Exec(query)
	return err
}

func main() {
	db_dsn := os.Getenv("ETCPING_DB_DSN")
	if len(db_dsn) == 0 {
		fmt.Println("ETCPING_DB_DSN env var not found.")
		return
	}

	var monitor_loop = time.Tick(20 * time.Second)

	signal_source := make(chan os.Signal)
	signal.Notify(signal_source, syscall.SIGHUP)

	for {
		restart := time.After(time.Minute * 10)
		db, err := sql.Open("mysql", db_dsn)
		if err != nil {
			fmt.Println(err)
			fmt.Println("Restarting in 30 seconds..")
			<-time.After(time.Second * 30)
			continue
		}
		err = db.Ping()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Restarting in 30 seconds..")
			<-time.After(time.Second * 30)
			continue
		}
		fmt.Println("Starting..")
		err = clear_alarms(db)
		if err != nil {
			fmt.Println(err)
		}
		err = create_alarms(db)
		if err != nil {
			fmt.Println(err)
		}
	SubLoop:
		for {
			select {
			case sig := <-signal_source:
				fmt.Println(sig)
				break SubLoop
			case <-restart:
				break SubLoop
			case <-monitor_loop:
				err = clear_alarms(db)
				if err != nil {
					fmt.Println(err)
				}
				err = create_alarms(db)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
		fmt.Println("Stopping..")
		db.Close()
	}
}
