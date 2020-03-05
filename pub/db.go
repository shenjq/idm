package pub

import (
	"database/sql"
	"idm/conf"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"net/url"
)

type DataConn struct {
	User    string
	Pass    string
	Host    string
	Dbname  string
	Charset string
	Db      *sql.DB
}

var gDB *sql.DB

func InitDB() (err error) {
	glog.V(3).Info("开始连接数据库...")
	c := conf.GetIni()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&loc=%s&parseTime=true",
		c.User, c.Pass, c.Addr, c.Port, c.Dbname, c.Charset, url.QueryEscape("Asia/Shanghai"))
	glog.V(3).Infof("[%s]\n", dsn)
	var db *sql.DB
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	err = db.Ping()
	if err != nil {
		return
	}
	if c.Maxconn > 0 {
		db.SetMaxOpenConns(c.Maxconn)
	} else {
		db.SetMaxOpenConns(20)
	}
	if c.Maxidle > 0 {
		db.SetMaxIdleConns(c.Maxidle)
	} else {
		db.SetMaxIdleConns(5)
	}

	glog.V(3).Info("连接数据库...完成")
	gDB = db
	return nil
}

func GetDb() *sql.DB {
	return gDB
}

func SelectCount(query string, args ...interface{}) int {
	var ct int
	QueryOneRow(query, args...).Scan(&ct)
	return ct
}
func QueryOneRow(query string, args ...interface{}) (row *sql.Row) {
	return gDB.QueryRow(query, args...)
}
