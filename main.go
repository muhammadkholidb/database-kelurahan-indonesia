package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-rod/rod"
)

type RowData struct {
	SubdistrictID   string
	PostalCode      string
	SubdistrictName string
	SubdistrictCode string
	DistrictName    string
	CityName        string
	ProvinceName    string
}

// Desa / Kelurahan
type Subdistrict struct {
	ID         int
	Name       string
	Code       string // Kode wilayah
	PostalCode string
	DistrictID int
}

// Kecamatan
type District struct {
	ID     int
	Name   string
	CityID int
}

// Kota / Kabupaten
type City struct {
	ID         int
	Name       string
	ProvinceID int
}

// Provinsi
type Province struct {
	ID   int
	Name string
}

// A reference about the definition of district in Indonesia: https://en.wikipedia.org/wiki/Districts_of_Indonesia

func main() {

	start := time.Now()

	totalSubdistrict := 83763

	perhal := 1000

	maxPage := int(math.Ceil(float64(totalSubdistrict) / float64(perhal)))

	url := fmt.Sprintf("https://www.nomor.net/_kodepos.php?_i=desa-kodepos&daerah=&jobs=&perhal=%d&sby=010000&asc=0001010&urut=8", perhal)

	browser := rod.New().MustConnect()
	defer browser.MustClose()

	rowData := []RowData{}
	rowData = append(rowData, parseRows(browser, url)...)

	no1 := 1
	no2 := perhal

	for kk := 2; kk <= maxPage; kk++ {

		time.Sleep(10 * time.Second)

		url = fmt.Sprintf("https://www.nomor.net/_kodepos.php?_i=desa-kodepos&daerah=&jobs=&perhal=%d&urut=8&asc=0001010&sby=010000&no1=%d&no2=%d&kk=%d", perhal, no1, no2, kk)

		rowData = append(rowData, parseRows(browser, url)...)

		no1 = no1 + perhal
		no2 = no2 + perhal
	}

	districtNames := map[string]int{}
	cityNames := map[string]int{}
	provinceNames := map[string]int{}

	subdistricts := []Subdistrict{}
	districts := []District{}
	cities := []City{}
	provinces := []Province{}

	for _, rd := range rowData {
		if _, ok := provinceNames[rd.ProvinceName]; !ok {
			provinceNames[rd.ProvinceName] = len(provinceNames) + 1
			provinces = append(provinces, Province{
				ID:   provinceNames[rd.ProvinceName],
				Name: rd.ProvinceName,
			})
		}
		if _, ok := cityNames[rd.CityName]; !ok {
			cityNames[rd.CityName] = len(cityNames) + 1
			cities = append(cities, City{
				ID:         cityNames[rd.CityName],
				Name:       rd.CityName,
				ProvinceID: provinceNames[rd.ProvinceName],
			})
		}
		// There are same district names in different cities
		districtCityName := rd.DistrictName + "-" + rd.CityName
		if _, ok := districtNames[districtCityName]; !ok {
			districtNames[districtCityName] = len(districtNames) + 1
			districts = append(districts, District{
				ID:     districtNames[districtCityName],
				Name:   rd.DistrictName,
				CityID: cityNames[rd.CityName],
			})
		}
		subdistrictID, _ := strconv.Atoi(rd.SubdistrictID)
		subdistricts = append(subdistricts, Subdistrict{
			ID:         subdistrictID,
			Name:       rd.SubdistrictName,
			Code:       rd.SubdistrictCode,
			PostalCode: rd.PostalCode,
			DistrictID: districtNames[districtCityName],
		})
	}

	log.Println("len(subdistricts):", len(subdistricts))
	log.Println("len(districts):", len(districts))
	log.Println("len(cities):", len(cities))
	log.Println("len(provinces):", len(provinces))

	writeSqlInsertSubdistrictsToPronvinces(rowData)
	writeSqlInsertProvinces(provinces)
	writeSqlInsertCities(cities)
	writeSqlInsertDistricts(districts)
	writeSqlInsertSubdistricts(subdistricts)

	end := time.Now()

	log.Println("Started at:", start)
	log.Println("Finished at:", end)
}

func parseRows(browser *rod.Browser, url string) []RowData {

	start := time.Now()

	log.Println("Opening page:", url)

	page := browser.MustPage(url).MustWaitStable()
	defer page.MustClose()

	rows, err := page.MustElement("tbody.header_mentok").MustNext().Describe(-1, false)
	if err != nil {
		log.Fatal("Describe error: ", err)
	}

	arr := []RowData{}
	for _, row := range rows.Children {
		cols := row.Children
		arr = append(arr, RowData{
			SubdistrictID:   strings.TrimSpace(cols[0].Children[0].NodeValue),
			PostalCode:      strings.TrimSpace(cols[1].Children[0].Children[0].NodeValue),
			SubdistrictName: strings.TrimSpace(cols[2].Children[0].Children[0].NodeValue),
			SubdistrictCode: strings.TrimSpace(cols[3].Children[0].Children[0].NodeValue),
			DistrictName:    strings.TrimSpace(cols[4].Children[0].Children[0].NodeValue),
			CityName:        strings.TrimSpace(cols[5].Children[0].NodeValue) + " " + strings.TrimSpace(cols[6].Children[0].Children[0].NodeValue),
			ProvinceName:    strings.TrimSpace(cols[7].Children[0].Children[0].NodeValue),
		})
	}

	log.Println("Parsed in:", time.Since(start))

	return arr
}

func writeSqlInsertSubdistrictsToPronvinces(rowData []RowData) {
	start := time.Now()
	filename := "subdistricts_to_provinces.sql"
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Create subdistricts sql file error: ", err)
	}
	sql := strings.Builder{}
	for _, rd := range rowData {
		sql.WriteString(fmt.Sprintf("insert into \"subdistricts_to_provinces\" (\"subdistrict_id\", \"subdistrict_name\", \"subdistrict_code\", \"postal_code\", \"district_name\", \"city_name\", \"province_name\") values (%s, '%s', '%s', '%s', '%s', '%s', '%s');\n", rd.SubdistrictID, escapeSingleQuote(rd.SubdistrictName), rd.SubdistrictCode, rd.PostalCode, escapeSingleQuote(rd.DistrictName), escapeSingleQuote(rd.CityName), escapeSingleQuote(rd.ProvinceName)))
	}
	n, err := file.WriteString(sql.String())
	if err != nil {
		log.Fatal("Write subdistricts to provinces sql error: ", err)
	}
	log.Printf("File '%s' has been created in %s with the size of %d bytes\n", filename, time.Since(start), n)
}

func writeSqlInsertSubdistricts(subdistricts []Subdistrict) {
	start := time.Now()
	filename := "subdistricts.sql"
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Create subdistricts sql file error: ", err)
	}
	sql := strings.Builder{}
	for _, s := range subdistricts {
		sql.WriteString(fmt.Sprintf("insert into \"subdistricts\" (\"id\", \"district_id\", \"name\", \"code\", \"postal_code\") values (%d, %d, '%s', '%s', '%s');\n", s.ID, s.DistrictID, escapeSingleQuote(s.Name), s.Code, s.PostalCode))
	}
	n, err := file.WriteString(sql.String())
	if err != nil {
		log.Fatal("Write subdistricts sql error: ", err)
	}
	log.Printf("File '%s' has been created in %s with the size of %d bytes\n", filename, time.Since(start), n)
}

func writeSqlInsertDistricts(districts []District) {
	start := time.Now()
	filename := "districts.sql"
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Create districts sql file error: ", err)
	}
	sql := strings.Builder{}
	for _, d := range districts {
		sql.WriteString(fmt.Sprintf("insert into \"districts\" (\"id\", \"city_id\", \"name\") values (%d, %d, '%s');\n", d.ID, d.CityID, escapeSingleQuote(d.Name)))
	}
	n, err := file.WriteString(sql.String())
	if err != nil {
		log.Fatal("Write districts sql error: ", err)
	}
	log.Printf("File '%s' has been created in %s with the size of %d bytes\n", filename, time.Since(start), n)
}

func writeSqlInsertCities(cities []City) {
	start := time.Now()
	filename := "cities.sql"
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Create cities sql file error: ", err)
	}
	sql := strings.Builder{}
	for _, c := range cities {
		sql.WriteString(fmt.Sprintf("insert into \"cities\" (\"id\", \"province_id\", \"name\") values (%d, %d, '%s');\n", c.ID, c.ProvinceID, escapeSingleQuote(c.Name)))
	}
	n, err := file.WriteString(sql.String())
	if err != nil {
		log.Fatal("Write cities sql error: ", err)
	}
	log.Printf("File '%s' has been created in %s with the size of %d bytes\n", filename, time.Since(start), n)
}

func writeSqlInsertProvinces(provinces []Province) {
	start := time.Now()
	filename := "provinces.sql"
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal("Create provinces sql file error: ", err)
	}
	sql := strings.Builder{}
	for _, p := range provinces {
		sql.WriteString(fmt.Sprintf("insert into \"provinces\" (\"id\", \"name\") values (%d, '%s');\n", p.ID, escapeSingleQuote(p.Name)))
	}
	n, err := file.WriteString(sql.String())
	if err != nil {
		log.Fatal("Write provinces sql error: ", err)
	}
	log.Printf("File '%s' has been created in %s with the size of %d bytes\n", filename, time.Since(start), n)
}

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
