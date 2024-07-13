package main

import (
	"database/sql"
	"encoding/csv"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type RDWRecord struct {
	Kenteken        string
	Voertuigsoort   string
	Merk            string
	Handelsbenaming string

	VervaldatumApk      time.Time
	DatumTenaamstelling time.Time

	BrutoBpm float32

	Inrichting        string
	AantalZitplaatsen int
	EersteKleur       string
	TweedeKleur       string

	AantalCilinders int
	Cilinderinhoud  int

	MassaLedigVoertuig             int
	ToegestaneMaximumMassaVoertuig int
	MassaRijklaar                  int
	MaximumTrekkenMassaOngeremd    int
	MaximumTrekkenMassaGeremd      int

	DatumEersteToelating        time.Time
	DatumEersteTenaamstallingNL time.Time
	WachtOpKeuren               string
	Catalogusprijs              float32
	WamVerzekerd                string

	MaxSnelheid    int
	Laadvermogen   int
	OpleggerGeremd int

	AanhangwagenAutonoomGeremd int
	AanhangwagenMiddenasGeremd int

	AantalStaanplaatsen int
	AantalDeuren        int
	AantalWielen        int

	AfstandHartKoppelingTotAchterzijdeVoertuig int
	AfstandVoorzijdeVoertuigTotHartKoppeling   int

	AfwijkendeMaximumSnelheid int

	Lengte  int
	Breedte int

	EuropeseVoertuigCategorie             string
	EuropeseVoertuigCategorieToevoeging   string
	EuropeseUitvoeringcategorieToevoeging string

	PlaatsChassisnummer        string
	TechnischeMaxMassaVoertuig int

	Type                   string
	TypeGasinstallatie     string
	Typegoedkeuringsnummer string
	Variant                string
	Uitvoering             string

	VolgnummerWijzigingEuTypegoedkeuring int
	VermoegenMassarijklaar               float32
	Wielbasis                            int

	exportIndicator                    string
	OpenstaandeTerugroepactieIndicator string

	VervaldatumTachograaf time.Time
	TaxiIndicator         string

	MaximumMassaSamenstelling     int
	AantalRolstoelplaatsen        int
	MaximumOndersteunendeSnelheid float32

	JaarLaatsteRegistratieTellerstand int
	Tellerstandoordeel                string
	CodeToelichtingTellerstandoordeel string
	TenaamstellenMogelijk             string

	VervaldatumApkDt                       time.Time
	DatumTenaamstellingDt                  time.Time
	DatumEersteToelatingDt                 time.Time
	DatumEersteTenaamstellingInNederlandDt time.Time
	VervaldatumTachograafDt                time.Time

	MaximumLastOnderDeVoorasSenTezamenKoppeling int
	TypeRemsysteemVoertuigCode                  string

	Rupsonderstelconfiguratiecode string
	WielbasisVoertuigMinimum      int
	WielbasisVoertuigMaximum      int

	LengteVoertuigMinimum  int
	LengteVoertuigMaximum  int
	BreedteVoertuigMinimum int
	BreedteVoertuigMaximum int

	HoogteVoertuig        float32
	HoogteVoertuigMinimum float32
	HoogteVoertuigMaximum float32

	MassaBedrijfsklaarMinimaal int
	MassaBedrijfsklaarMaximaal int

	TechnischToelaatbaarMassaKoppelpunt int
	MaximumMassaTechnischMaximaal       int
	MaximumMassaTechnischMinimaal       int

	SubcategorieNederland                         string
	VerticaleBelastingKoppelpuntGetrokkenVoertuig int
	Zuinigheidsclassificatie                      string

	RegistratieDatumGoedkeuringAfschrijvingsmomentBpm   time.Time
	RegistratieDatumGoedkeuringAfschrijvingsmomentBpmDt time.Time

	GemLadingWrde float32
	AerodynVoorz  string
	MassaAltAandr int
	VerlCabInd    string

	ApiGekentekendeVoertuigenAssen                string
	ApiGekentekendeVoertuigenBrandstof            string
	ApiGekentekendeVoertuigenCarrosserie          string
	ApiGekentekendeVoertuigenCarrosserieSpecifiek string
	ApiGekentekendeVoertuigenVoertuigklasse       string
}

func main() {
	defer timeTrack(time.Now(), "CSV processing")
	log.Println("Starting CSV processing")

	file, err := os.Open("rdw-1m.csv")
	if err != nil {
		log.Fatal("Error opening file", err)
	}
	defer file.Close()

	db, err := connectToDB()
	if err != nil {
		log.Fatal("Error connecting to the database ", err)
	}

	reader := csv.NewReader(file)
	// Read and discard the first line (header)
	if _, err := reader.Read(); err != nil {
		log.Fatal("Error reading the header line", err)
	}

	const batchSize = 2000
	var batch []RDWRecord
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 100)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal("Error reading a record", err)
		}

		// Convert the record to a typed record
		rdwRecord := NewRDWRecord(record)

		batch = append(batch, rdwRecord)
		if len(batch) >= batchSize {
			wg.Add(1)
			semaphore <- struct{}{} // Acquire a slot in the semaphore
			go func(batch []RDWRecord) {
				defer wg.Done()
				processRecords(batch, db)
				<-semaphore // Release a slot in the semaphore
			}(batch)
			batch = nil // Start a new batch
		}
	}

	// Process any remaining records that did not make a full batch
	if len(batch) > 0 {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(batch []RDWRecord) {
			defer wg.Done()
			processRecords(batch, db)
			<-semaphore
		}(batch)
	}

	wg.Wait() // Wait for all goroutines to finish
	log.Println("File processed successfully")
}

func processRecords(records []RDWRecord, db *sql.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Error starting transaction", err)
	}

	stmt, err := tx.Prepare(
		"INSERT INTO voertuigen (" +
			"kenteken, voertuigsoort, merk, handelsbenaming, " +
			"vervaldatum_apk, datum_tenaamstelling, " +
			"bruto_bpm, " +
			"inrichting, aantal_zitplaatsen, eerste_kleur, tweede_kleur, " +
			"aantal_cilinders, cilinderinhoud, " +
			"massa_ledig_voertuig, toegestane_maximum_massa_voertuig, massa_rijklaar, maximum_massa_trekken_ongeremd, maximum_trekken_massa_geremd, " +
			"datum_eerste_toelating, datum_eerste_tenaamstelling_in_nederland, " +
			"wacht_op_keuren, catalogusprijs, wam_verzekerd, " +
			"maximale_constructiesnelheid, laadvermogen, oplegger_geremd, aanhangwagen_autonoom_geremd, aanhangwagen_middenas_geremd, " +
			"aantal_staanplaatsen, aantal_deuren, aantal_wielen, " +
			"afstand_hart_koppeling_tot_achterzijde_voertuig, afstand_voorzijde_voertuig_tot_hart_koppeling, " +
			"afwijkende_maximum_snelheid, lengte, breedte, " +
			"europese_voertuigcategorie, europese_voertuigcategorie_toevoeging, europese_uitvoeringcategorie_toevoeging, " +
			"plaats_chassisnummer, technische_max_massa_voertuig, " +
			"type, type_gasinstallatie, typegoedkeuringsnummer, variant, uitvoering, " +
			"volgnummer_wijziging_eu_typegoedkeuring, " +
			"vermogen_massarijklaar, wielbasis, " +
			"export_indicator, openstaande_terugroepactie_indicator, " +
			"vervaldatum_tachograaf, taxi_indicator, " +
			"maximum_massa_samenstelling, aantal_rolstoelplaatsen, maximum_ondersteunende_snelheid, " +
			"jaar_laatste_registratie_tellerstand, tellerstandoordeel, code_toelichting_tellerstandoordeel, tenaamstellen_mogelijk, " +
			"vervaldatum_apk_dt, datum_tenaamstelling_dt, datum_eerste_toelating_dt, datum_eerste_tenaamstelling_in_nederland_dt, vervaldatum_tachograaf_dt, " +
			"maximum_last_onder_de_vooras_sen_tezamen_koppeling, type_remsysteem_voertuig_code, " +
			"rupsonderstelconfiguratiecode, wielbasis_voertuig_minimum, wielbasis_voertuig_maximum, " +
			"lengte_voertuig_minimum, lengte_voertuig_maximum, " +
			"breedte_voertuig_minimum, breedte_voertuig_maximum, " +
			"hoogte_voertuig, hoogte_voertuig_minimum, hoogte_voertuig_maximum, " +
			"massa_bedrijfsklaar_minimaal, massa_bedrijfsklaar_maximaal, " +
			"technisch_toelaatbaar_massa_koppelpunt, " +
			"maximum_massa_technisch_maximaal, maximum_massa_technisch_minimaal, " +
			"subcategorie_nederland, verticale_belasting_koppelpunt_getrokken_voertuig, zuinigheidsclassificatie, " +
			"registratie_datum_goedkeuring_afschrijvingsmoment_bpm, registratie_datum_goedkeuring_afschrijvingsmoment_bpm_dt, " +
			"gem_lading_wrde, aerodyn_voorz, massa_alt_aandr, verl_cab_ind, " +
			"api_gekentekende_voertuigen_assen, api_gekentekende_voertuigen_brandstof, api_gekentekende_voertuigen_carrosserie, api_gekentekende_voertuigen_carrosserie_specifiek, api_gekentekende_voertuigen_voertuigklasse) " +
			"VALUES (?, ?, ?, ?, " +
			"?, ?, " +
			"?, " +
			"?, ?, ?, ?, " +
			"?, ?, " +
			"?, ?, ?, ?, " +
			"?, ?, " +
			"?, ?, ?, " +
			"?, ?, ?, ?, ?, " +
			"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
	)

	if err != nil {
		log.Fatal("Error preparing statement", err)
	}

	defer stmt.Close() // Ensure the statement is closed

	for _, record := range records {
		_, err = stmt.Exec(
			record.Kenteken, record.Voertuigsoort, record.Merk, record.Handelsbenaming,
			record.VervaldatumApk, record.DatumTenaamstelling,
			record.BrutoBpm,
			record.Inrichting, record.AantalZitplaatsen, record.EersteKleur, record.TweedeKleur,
			record.AantalCilinders, record.Cilinderinhoud,
			record.MassaLedigVoertuig, record.ToegestaneMaximumMassaVoertuig, record.MassaRijklaar, record.MaximumTrekkenMassaOngeremd, record.MaximumTrekkenMassaGeremd,
			record.DatumEersteToelating, record.DatumEersteTenaamstallingNL,
			record.WachtOpKeuren, record.Catalogusprijs, record.WamVerzekerd,
			record.MaxSnelheid, record.Laadvermogen, record.OpleggerGeremd, record.AanhangwagenAutonoomGeremd, record.AanhangwagenMiddenasGeremd,
			record.AantalStaanplaatsen, record.AantalDeuren, record.AantalWielen,
			record.AfstandHartKoppelingTotAchterzijdeVoertuig, record.AfstandVoorzijdeVoertuigTotHartKoppeling,
			record.AfwijkendeMaximumSnelheid, record.Lengte, record.Breedte,
			record.EuropeseVoertuigCategorie, record.EuropeseVoertuigCategorieToevoeging, record.EuropeseUitvoeringcategorieToevoeging,
			record.PlaatsChassisnummer, record.TechnischeMaxMassaVoertuig,
			record.Type, record.TypeGasinstallatie, record.Typegoedkeuringsnummer, record.Variant, record.Uitvoering,
			record.VolgnummerWijzigingEuTypegoedkeuring,
			record.VermoegenMassarijklaar, record.Wielbasis,
			record.exportIndicator, record.OpenstaandeTerugroepactieIndicator,
			record.VervaldatumTachograaf, record.TaxiIndicator,
			record.MaximumMassaSamenstelling, record.AantalRolstoelplaatsen, record.MaximumOndersteunendeSnelheid,
			record.JaarLaatsteRegistratieTellerstand, record.Tellerstandoordeel, record.CodeToelichtingTellerstandoordeel, record.TenaamstellenMogelijk,
			record.VervaldatumApkDt, record.DatumTenaamstellingDt, record.DatumEersteToelatingDt, record.DatumEersteTenaamstellingInNederlandDt, record.VervaldatumTachograafDt,
			record.MaximumLastOnderDeVoorasSenTezamenKoppeling, record.TypeRemsysteemVoertuigCode,
			record.Rupsonderstelconfiguratiecode, record.WielbasisVoertuigMinimum, record.WielbasisVoertuigMaximum,
			record.LengteVoertuigMinimum, record.LengteVoertuigMaximum,
			record.BreedteVoertuigMinimum, record.BreedteVoertuigMaximum,
			record.HoogteVoertuig, record.HoogteVoertuigMinimum, record.HoogteVoertuigMaximum,
			record.MassaBedrijfsklaarMinimaal, record.MassaBedrijfsklaarMaximaal,
			record.TechnischToelaatbaarMassaKoppelpunt,
			record.MaximumMassaTechnischMaximaal, record.MaximumMassaTechnischMinimaal,
			record.SubcategorieNederland, record.VerticaleBelastingKoppelpuntGetrokkenVoertuig, record.Zuinigheidsclassificatie,
			record.RegistratieDatumGoedkeuringAfschrijvingsmomentBpm, record.RegistratieDatumGoedkeuringAfschrijvingsmomentBpmDt,
			record.GemLadingWrde, record.AerodynVoorz, record.MassaAltAandr, record.VerlCabInd,
			record.ApiGekentekendeVoertuigenAssen, record.ApiGekentekendeVoertuigenBrandstof, record.ApiGekentekendeVoertuigenCarrosserie, record.ApiGekentekendeVoertuigenCarrosserieSpecifiek, record.ApiGekentekendeVoertuigenVoertuigklasse,
		)
		if err != nil {
			log.Println("Error inserting record ", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		log.Fatal("Error committing transaction ", err)
	}

}

func NewRDWRecord(record []string) RDWRecord {
	return RDWRecord{
		Kenteken:                       record[0],
		Voertuigsoort:                  record[1],
		Merk:                           record[2],
		Handelsbenaming:                record[3],
		VervaldatumApk:                 parseDateRdwFormat(record[4]),
		DatumTenaamstelling:            parseDateRdwFormat(record[5]),
		BrutoBpm:                       stringToDecimal(record[6]),
		Inrichting:                     record[7],
		AantalZitplaatsen:              stringToInt(record[8]),
		EersteKleur:                    record[9],
		TweedeKleur:                    record[10],
		AantalCilinders:                stringToInt(record[11]),
		Cilinderinhoud:                 stringToInt(record[12]),
		MassaLedigVoertuig:             stringToInt(record[13]),
		ToegestaneMaximumMassaVoertuig: stringToInt(record[14]),
		MassaRijklaar:                  stringToInt(record[15]),
		MaximumTrekkenMassaOngeremd:    stringToInt(record[16]),
		MaximumTrekkenMassaGeremd:      stringToInt(record[17]),
		DatumEersteToelating:           parseDateRdwFormat(record[18]),
		DatumEersteTenaamstallingNL:    parseDateRdwFormat(record[19]),
		WachtOpKeuren:                  record[20],
		Catalogusprijs:                 stringToDecimal(record[21]),
		WamVerzekerd:                   record[22],
		MaxSnelheid:                    stringToInt(record[23]),
		Laadvermogen:                   stringToInt(record[24]),
		OpleggerGeremd:                 stringToInt(record[25]),
		AanhangwagenAutonoomGeremd:     stringToInt(record[26]),
		AanhangwagenMiddenasGeremd:     stringToInt(record[27]),
		AantalStaanplaatsen:            stringToInt(record[28]),
		AantalDeuren:                   stringToInt(record[29]),
		AantalWielen:                   stringToInt(record[30]),
		AfstandHartKoppelingTotAchterzijdeVoertuig:          stringToInt(record[31]),
		AfstandVoorzijdeVoertuigTotHartKoppeling:            stringToInt(record[32]),
		AfwijkendeMaximumSnelheid:                           stringToInt(record[33]),
		Lengte:                                              stringToInt(record[34]),
		Breedte:                                             stringToInt(record[35]),
		EuropeseVoertuigCategorie:                           record[36],
		EuropeseVoertuigCategorieToevoeging:                 record[37],
		EuropeseUitvoeringcategorieToevoeging:               record[38],
		PlaatsChassisnummer:                                 record[39],
		TechnischeMaxMassaVoertuig:                          stringToInt(record[40]),
		Type:                                                record[41],
		TypeGasinstallatie:                                  record[42],
		Typegoedkeuringsnummer:                              record[43],
		Variant:                                             record[44],
		Uitvoering:                                          record[45],
		VolgnummerWijzigingEuTypegoedkeuring:                stringToInt(record[46]),
		VermoegenMassarijklaar:                              stringToDecimal(record[47]),
		Wielbasis:                                           stringToInt(record[48]),
		exportIndicator:                                     record[49],
		OpenstaandeTerugroepactieIndicator:                  record[50],
		VervaldatumTachograaf:                               parseDateRdwFormat(record[51]),
		TaxiIndicator:                                       record[52],
		MaximumMassaSamenstelling:                           stringToInt(record[53]),
		AantalRolstoelplaatsen:                              stringToInt(record[54]),
		MaximumOndersteunendeSnelheid:                       stringToDecimal(record[55]),
		JaarLaatsteRegistratieTellerstand:                   stringToInt(record[56]),
		Tellerstandoordeel:                                  record[57],
		CodeToelichtingTellerstandoordeel:                   record[58],
		TenaamstellenMogelijk:                               record[59],
		VervaldatumApkDt:                                    parseISO8601Date(record[60]),
		DatumTenaamstellingDt:                               parseISO8601Date(record[61]),
		DatumEersteToelatingDt:                              parseISO8601Date(record[62]),
		DatumEersteTenaamstellingInNederlandDt:              parseISO8601Date(record[63]),
		VervaldatumTachograafDt:                             parseISO8601Date(record[64]),
		MaximumLastOnderDeVoorasSenTezamenKoppeling:         stringToInt(record[65]),
		TypeRemsysteemVoertuigCode:                          record[66],
		Rupsonderstelconfiguratiecode:                       record[67],
		WielbasisVoertuigMinimum:                            stringToInt(record[68]),
		WielbasisVoertuigMaximum:                            stringToInt(record[69]),
		LengteVoertuigMinimum:                               stringToInt(record[70]),
		LengteVoertuigMaximum:                               stringToInt(record[71]),
		BreedteVoertuigMinimum:                              stringToInt(record[72]),
		BreedteVoertuigMaximum:                              stringToInt(record[73]),
		HoogteVoertuig:                                      stringToDecimal(record[74]),
		HoogteVoertuigMinimum:                               stringToDecimal(record[75]),
		HoogteVoertuigMaximum:                               stringToDecimal(record[76]),
		MassaBedrijfsklaarMinimaal:                          stringToInt(record[77]),
		MassaBedrijfsklaarMaximaal:                          stringToInt(record[78]),
		TechnischToelaatbaarMassaKoppelpunt:                 stringToInt(record[79]),
		MaximumMassaTechnischMaximaal:                       stringToInt(record[80]),
		MaximumMassaTechnischMinimaal:                       stringToInt(record[81]),
		SubcategorieNederland:                               record[82],
		VerticaleBelastingKoppelpuntGetrokkenVoertuig:       stringToInt(record[83]),
		Zuinigheidsclassificatie:                            record[84],
		RegistratieDatumGoedkeuringAfschrijvingsmomentBpm:   parseDateRdwFormat(record[85]),
		RegistratieDatumGoedkeuringAfschrijvingsmomentBpmDt: parseISO8601Date(record[86]),
		GemLadingWrde:                                       stringToDecimal(record[87]),
		AerodynVoorz:                                        record[88],
		MassaAltAandr:                                       stringToInt(record[89]),
		VerlCabInd:                                          record[90],
		ApiGekentekendeVoertuigenAssen:                      record[91],
		ApiGekentekendeVoertuigenBrandstof:                  record[92],
		ApiGekentekendeVoertuigenCarrosserie:                record[93],
		ApiGekentekendeVoertuigenCarrosserieSpecifiek:       record[94],
		ApiGekentekendeVoertuigenVoertuigklasse:             record[95],
	}
}
