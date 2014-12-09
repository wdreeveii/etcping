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

func (e dataEntry) String() string {
	const dtlayout = "2006-01-02 15:04:05"
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
	query := `CREATE TABLE hosts_new LIKE hosts`
	_, err := db.Exec(query)
	if err != nil {
		fmt.Println(err)
		return
	}

	start := true
	query = `REPLACE INTO hosts_new (ip, alias, primary_alias, dt) VALUES `
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

	query = `RENAME TABLE hosts TO hosts_old, hosts_new TO hosts`
	_, err = db.Exec(query)
	if err != nil {
		fmt.Println(err)
		return
	}

	query = `DROP TABLE hosts_old`
	_, err = db.Exec(query)
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}

func main() {
	db_dsn := os.Getenv("ETCPING_DB_DSN")
	if len(db_dsn) == 0 {
		fmt.Println("ETCPING_DB_DSN env var not found.")
		return
	}

	var max_storage int = 5000
	max_storage_str := os.Getenv("ETCPING_MAX_STORAGE")
	if len(max_storage_str) != 0 {
		a, err := strconv.ParseInt(max_storage_str, 10, 32)
		if err == nil {
			max_storage = int(a)
		} else {
			fmt.Println(err)
		}
	}

	var data_dump = make(chan dataEntry, 100)
	p := fastping.NewPinger()
	p.Debug = false
	p.MaxRTT = time.Second * 60
	p.OnRecv = func(addr *net.IPAddr, t time.Duration) {
		record := dataEntry{time.Now(), addr.String(), t}
		data_dump <- record
	}
	p.OnIdle = func() {
		fmt.Println("idle")
	}

	var started bool = false
	var data_storage []dataEntry
	var batch_insert_ticker = time.Tick(5 * time.Second)

	var hosts = make(hostList)

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

		go update_hosts_table(db, hosts_update)

		for host_ip, host_definition := range hosts_update {
			_, exists := hosts[host_ip]
			if !exists {
				hosts[host_ip] = host_definition
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
				save_start := len(data_storage) - max_storage
				if save_start < 0 {
					save_start = 0
				}
				data_storage = append(data_storage[save_start:], record)
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
