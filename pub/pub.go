package pub

import "time"

const (
	timeTemplate1 = "2006-01-02 15:04:05" //常规类型
	timeTemplate2 = "2006/01/02 15:04:05" //其他类型
	timeTemplate3 = "2006-01-02"          //其他类型
	timeTemplate4 = "20060102"
	timeTemplate5 = "15:04:05" //其他类型
)

//timeTemplate1 = "2006-01-02 15:04:05"
func GetFmtDateStr1(d time.Time) (s string) {
	return d.Format(timeTemplate1)
}

func GetFmtDateStr4(d time.Time) (s string) {
	return d.Format(timeTemplate4)
}

//timeTemplate4 = "20060102"
func GetDateStr4() (s string) {
	return time.Now().Format(timeTemplate4)
}

func GetNextDateStr4() (s string) {
	return time.Now().Add(24 * time.Hour).Format(timeTemplate4)
}

func IsMonthEnd(s string) (bool, error) {
	d1, err := time.Parse(timeTemplate4, s)
	if err != nil {
		return false, err
	}
	d2 := d1.Add(time.Hour * 24)
	if d1.Month() != d2.Month() {
		return true, nil
	} else {
		return false, nil
	}
}

func IsYearEnd(s string) (bool, error) {
	d1, err := time.Parse(timeTemplate4, s)
	if err != nil {
		return false, err
	}
	d2 := d1.Add(time.Hour * 24)
	if d1.Year() != d2.Year() {
		return true, nil
	} else {
		return false, nil
	}
}

func GetCurrentPath() (string, error) {
	return "", nil
}
