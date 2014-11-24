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

func updateHostDefinition(db *sql.DB, host_ip string, host_definition host) error {
	query := `REPLACE INTO hosts (ip, alias, primary_alias, dt) VALUES `
	query += "('" + host_ip + "','" + host_definition.primary + "', 1, NOW())"
	for _, v := range host_definition.aliases {
		query += ",('" + host_ip + "','" + v + "', 0, NOW())"
	}
	fmt.Println(query)
	_, err := db.Exec(query)
	return err
}

func batchInsertData(db *sql.DB, storage []dataEntry) ([]dataEntry, error) {
	if len(storage) == 0 {
		return storage, nil
	}

	query := `REPLACE INTO pingdata (dt, host, duration) VALUES `
	for k, v := range storage {
		if k != 0 {
			query += ","
		}

		query += v.String()
	}
	fmt.Println(query)
	_, err := db.Exec(query)
	if err != nil {
		return storage, err
	}
	return nil, nil
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
	p.MaxRTT = time.Second * 10
	p.OnRecv = func(addr *net.IPAddr, t time.Duration) {
		record := dataEntry{time.Now(), addr.String(), t}
		fmt.Println("record:", record)
		data_dump <- record
	}
	p.OnIdle = func() {
		fmt.Println("idle")
	}
	p.RunLoop()
	/*<-p.Done()
	if err := p.Err(); err != nil {
		fmt.Println(err)
		return
	}*/

	var data_storage []dataEntry
	var batch_insert_ticker = time.Tick(5 * time.Second)

	var hosts = make(hostList)

	signal_source := make(chan os.Signal)
	signal.Notify(signal_source, syscall.SIGHUP)

	for {

		restart := time.After(1 * time.Minute)

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

		for host_ip, host_definition := range hosts_update {
			old_host_definition, exists := hosts[host_ip]
			if !exists {
				fmt.Println(host_ip, host_definition)
				hosts[host_ip] = host_definition
				err = p.AddIP(host_ip)
				if err != nil {
					fmt.Println(err)
					return
				}
				if database_working {
					err = updateHostDefinition(db, host_ip, host_definition)
					if err != nil {
						fmt.Println(err)
					}
				}
			} else {
				known_aliases := append(old_host_definition.aliases, old_host_definition.primary)
				for _, alias := range host_definition.aliases {
					if !stringInSlice(alias, known_aliases) {
						tmp := hosts[host_ip]
						tmp.aliases = append(hosts[host_ip].aliases, alias)
						hosts[host_ip] = tmp

						if database_working {
							err = updateHostDefinition(db, host_ip, host_definition)
							if err != nil {
								fmt.Println(err)
							}
						}
					}
				}
			}

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
						return
					}
				}
			}
		}
		fmt.Println("Stopping...")
		//p.Stop()
		db.Close()
	}
}
