package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/wdreeveii/go-fastping"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

type hostList map[string]host
type host struct {
	primary string
	aliases []string
}

func readEtcHosts(hosts hostList) error {
	etc_hosts, err := ioutil.ReadFile("/etc/hosts")
	if err != nil {
		return err
	}

	validIPAddr := regexp.MustCompile(`^[0-9A-Fa-f\.:]+$`)
	validFQDN := regexp.MustCompile(`^[-0-9A-Za-z\.]+$`)

	etc_lines := strings.SplitN(string(etc_hosts), "\n", -1)
ReadLines:
	for _, line := range etc_lines {
		fields := strings.Fields(line)

		// omit blacklisted ip addresses such as localhost
		for _, v := range fields {
			if v == "#blacklist" {
				continue ReadLines
			}
		}

		// omit lines with fewer than 2 fields
		if len(fields) < 2 {
			continue
		}

		// omit line if the first field is not a valid ip address
		if !validIPAddr.MatchString(fields[0]) {
			continue
		}

		host_definition := hosts[fields[0]]
		for ii := 1; ii < len(fields); ii++ {
			if !validFQDN.MatchString(fields[ii]) {
				break
			}
			if len(host_definition.primary) == 0 {
				host_definition.primary = fields[ii]
			} else if !stringInSlice(fields[ii], host_definition.aliases) {
				host_definition.aliases = append(host_definition.aliases, fields[ii])
			}
		}
		hosts[fields[0]] = host_definition
	}
	return nil
}

type dataEntry struct {
	dt       time.Time
	host     string
	duration time.Duration
}

const dtlayout = "2006-01-02 15:04:05"

func (e dataEntry) String() string {
	return "('" +
		e.dt.Format(dtlayout) +
		"','" + e.host + "'," +
		fmt.Sprintf("%v", float64(e.duration)/float64(time.Millisecond)) + ")"
}

func batchInsertData(db *sql.DB, storage []dataEntry) ([]dataEntry, error) {
	if len(storage) == 0 {
		return storage, nil
	}
	fmt.Println("Inserting", len(storage), "records.")
	query := `INSERT INTO pingdata (dt, host, duration) VALUES `
	for k, v := range storage {
		if k != 0 {
			query += ","
		}

		query += v.String()
	}
	_, err := db.Exec(query)
	if err != nil {
		return storage, err
	}
	return nil, nil
}

func update_hosts_table(db *sql.DB, hosts hostList) {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS hosts_new LIKE hosts`)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = db.Exec(`TRUNCATE hosts_new`)
	if err != nil {
		fmt.Println(err)
		return
	}

	start := true
	query := `REPLACE INTO hosts_new (ip, alias, primary_alias, dt) VALUES `
	for ip, host_data := range hosts {
		if !start {
			query += `,`
		} else {
			start = false
		}
		query += `('` + ip + `','` + host_data.primary + `',1,NOW())`
		for _, alias := range host_data.aliases {
			query += `,('` + ip + `','` + alias + `',0,NOW())`
		}
	}
	_, err = db.Exec(query)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = db.Exec(`RENAME TABLE hosts TO hosts_old, hosts_new TO hosts`)
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = db.Exec(`DROP TABLE hosts_old`)
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}

type host_info struct {
	LastReply         time.Time
	LastReplyDuration time.Duration
	Alarm             bool
}

func main() {
	db_dsn := os.Getenv("ETCPING_DB_DSN")
	if len(db_dsn) == 0 {
		fmt.Println("ETCPING_DB_DSN env var not found.")
		return
	}

	var max_storage int = 5000000
	max_storage_str := os.Getenv("ETCPING_MAX_STORAGE")
	if len(max_storage_str) != 0 {
		a, err := strconv.ParseInt(max_storage_str, 10, 32)
		if err == nil {
			max_storage = int(a)
		} else {
			fmt.Println(err)
		}
	}

	var data_dump = make(chan dataEntry, 10000)
	p := fastping.NewPinger()
	p.Debug = false
	p.MaxRTT = time.Second * 30
	p.OnRecv = func(addr *net.IPAddr, t time.Duration) {
		record := dataEntry{time.Now(), addr.String(), t}
		data_dump <- record
	}
	p.OnIdle = func() {
		fmt.Println("idle")
	}

	var started bool = false
	var data_storage []dataEntry
	var clear_storage = make(map[string]time.Time)
	var batch_insert_ticker = time.Tick(5 * time.Second)
	var process_alarms_ticker = time.Tick(30 * time.Second)

	var hosts = make(map[string]host_info)

	signal_source := make(chan os.Signal)
	signal.Notify(signal_source, syscall.SIGHUP)

	for {

		restart := time.After(10 * time.Minute)

		var database_working = true
		db, err := sql.Open("mysql", db_dsn)
		if err != nil {
			fmt.Println(err)
			database_working = false
		}
		err = db.Ping()
		if err != nil {
			fmt.Println(err)
			database_working = false
		}
		if !database_working {
			fmt.Println("Storing data temporarily until DB comes back.")
		}

		var hosts_update = make(hostList)
		err = readEtcHosts(hosts_update)
		if err != nil {
			fmt.Println(err)
		}
		if database_working {
			go update_hosts_table(db, hosts_update)
		}
		for host_ip, _ := range hosts_update {
			_, exists := hosts[host_ip]
			if !exists {
				hosts[host_ip] = host_info{}
				err = p.AddIP(host_ip)
				if err != nil {
					fmt.Println(err)
					return
				}
			}
		}

		if !started {
			p.RunLoop()
			started = true
		}
		fmt.Println("Starting....")

	SubLoop:
		for {
			select {
			case sig := <-signal_source:
				fmt.Println(sig)
				break SubLoop
			case <-restart:
				break SubLoop
			case record := <-data_dump:
				//fmt.Println(record)
				if record.dt.After(hosts[record.host].LastReply) {
					tmp := hosts[record.host]

					if tmp.Alarm {
						clear_storage[record.host] = record.dt
						tmp.Alarm = false
					}
					tmp.LastReply = record.dt
					tmp.LastReplyDuration = record.duration

					hosts[record.host] = tmp
				}
				save_start := len(data_storage) - max_storage
				if save_start < 0 {
					save_start = 0
				}
				data_storage = append(data_storage[save_start:], record)
			case current_time := <-process_alarms_ticker:
				if database_working {
					var current_alarms = make(map[string]time.Time)
					for ip, info := range hosts {
						if info.LastReply.Before(current_time.Add(-1 * time.Minute)) {
							tmp := hosts[ip]
							tmp.Alarm = true
							hosts[ip] = tmp
							current_alarms[ip] = info.LastReply
						}
					}
					var current_clears = make(map[string]time.Time)
					for k, v := range clear_storage {
						current_clears[k] = v
						delete(clear_storage, k)
					}
					store_alarm_state(db, current_alarms, current_clears)
				}
			case <-batch_insert_ticker:
				if database_working {
					data_storage, err = batchInsertData(db, data_storage)
					if err != nil {
						fmt.Println(err)
					}
				}
			}
		}
		fmt.Println("Stopping...")
		//p.Stop()
		db.Close()
	}
}
func create_new_alarms(db *sql.DB, current_alarms map[string]time.Time) error {
	var create_tmp_table = `
CREATE TEMPORARY TABLE IF NOT EXISTS alarms_update
LIKE alarms`
	_, err := db.Exec(create_tmp_table)
	if err != nil {
		return err
	}

	var truncate_tmp_table = `
TRUNCATE alarms_update`
	_, err = db.Exec(truncate_tmp_table)
	if err != nil {
		return err
	}

	var create_alarms = `
INSERT IGNORE INTO alarms_update (ip, dtStart) VALUES
`
	var start = true
	for k, v := range current_alarms {
		if !start {
			create_alarms += ","
		} else {
			start = false
		}
		create_alarms += "('" + k + "','" + v.Format(dtlayout) + "')"
	}
	_, err = db.Exec(create_alarms)
	if err != nil {
		return err
	}

	var apply_new_alarms = `
INSERT INTO alarms (ip, dtStart)
SELECT alarms_update.ip,
       alarms_update.dtStart
FROM alarms_update
LEFT JOIN alarms
ON alarms_update.ip = alarms.ip
AND alarms.dtEnd IS NULL
WHERE alarms.dtStart IS NULL
ON DUPLICATE KEY UPDATE dtEnd = NULL
`
	_, err = db.Exec(apply_new_alarms)
	if err != nil {
		return err
	}
	var drop_tmp = `
DROP TABLE alarms_update`
	_, err = db.Exec(drop_tmp)
	return err
}

func store_alarm_state(db *sql.DB, current_alarms map[string]time.Time, current_clears map[string]time.Time) {
	var err error
	fmt.Println("creating new alarms", len(current_alarms), time.Now())
	if len(current_alarms) > 0 {
		err = create_new_alarms(db, current_alarms)
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("closing alarms precise", len(current_clears), time.Now())
	var current_clear_query = `
UPDATE alarms SET dtEnd = ? WHERE ip = ? AND alarms.hold != 1`
	for k, v := range current_clears {
		_, err = db.Exec(current_clear_query, v.Format(dtlayout), k)
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("closing alarms general", time.Now())
	var clear_others_query = `
UPDATE alarms SET dtEnd = NOW()
WHERE dtEnd IS NULL
AND hold != b'1'`
	if len(current_alarms) > 0 {
		clear_others_query += `AND ip NOT IN (`
		var start = true
		for k, _ := range current_alarms {
			if !start {
				clear_others_query += ","
			} else {
				start = false
			}
			clear_others_query += "'" + k + "'"
		}
		clear_others_query += ")"
	}
	_, err = db.Exec(clear_others_query)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("store alarm state done", time.Now())
}
