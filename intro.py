import os

# Default save location
save_location = 'henkautot-raportti.txt'

# A helper value to show the order number of a table which is being calculated
order_next = 1

# Shows information about the program
def info():
    info = '\n* Tämä ohjelma näyttää Suomen henkilöautokannan keskeisiä ominaisuuksia *\n\n'
    info += '- Ohjelman ajamiseksi tarvitset Pythonin, Javan, Apache Sparkin, pyspark-kirjaston\n'
    info += '  sekä ajoneuvorekisteridatan csv-muodossa.\n'
    info += '- Ohjelman ajo voi kestää useita minuutteja.\n'
    info += '- Tulostaulukot näytetään konsolissa sekä kirjoitetaan tekstitiedostoon.'
    return info

# Shows a message about tables being calculated
def waitMessage(table_count):
    waitMessage = 'Haetaan ja lasketaan arvoja...\n'
    waitMessage += (f'Taulukoita on {table_count}, ja kunkin luominen voi kestää noin minuutin')
    print(waitMessage)

# Shows the number and the name of the table being calculated
def runInfo(next):
    global order_next
    run_info = (f'\nSeuraavaksi taulukko numero {order_next}: {next}\n')
    print(run_info)
    order_next += 1

# Shows needed background information (in the report)
def report_info():
    report_info = 'SUOMEN HENKILÖAUTOKANNAN KESKEISIÄ TIETOJA\n\n'
    report_info += 'Yleisimpien koodien selitteitä:\n\n'
    report_info += '- Ajoneuvon käyttö: 01=yksityinen\n'
    report_info += '- Korityyppi: AC=farmari, AB=viistoperä, AF=monikäyttöajoneuvo, AA=sedan\n'
    report_info += '- Käyttövoima: 01=bensiini, 02=dieselöljy, 04=sähkö\n'
    report_info += '- Sähköhybridin luokka: 2=pelkästään polttomoottorilla ladattava, 1=sähköverkosta ladattava\n'
    report_info += '- Vaihteisto: 2=automaattinen, 1=käsivalintainen, 3=portaaton\n\n'
    report_info += 'WLTP2_Co2 = hiilidioksidipäästöt, painotettu WLTP\n'
    report_info += 'Huom. merkkien ja mallien kohdalla sama merkki/malli voi olla jakautunut useammalle riville\n\n\n'
    return report_info

# Confirms that the user wants to begin a run and gets/confirms a report file's name/location
def confirmRun():
    global save_location
    print(info())
    while True: 
        inp = input('\nJatketaanko? (K/E): ')
        if inp in ['K', 'k']:
            needSaveLocation = True
            while needSaveLocation:
                print(f'\nTallennustiedoston sijainti on {save_location}. Paina Enter, jos hyväksyt,')
                inp = input('tai syötä tallennustiedoston polku nimineen (esim. C:/temp/rap.txt): ')
                if (inp == ''):
                    needSaveLocation = False
                else:
                    save_location = inp
            try:
                file = open(save_location, 'w', encoding='utf-8')
                file.close()
                os.remove(save_location)
                return save_location
            except:
                print('Tallennustiedoston luominen polkuun ei onnistunut')
        elif (inp in ['E', 'e']):
            print('')
            quit()