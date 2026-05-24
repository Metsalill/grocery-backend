import psycopg2
import os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

translations = {
    '52840': """Loputa karpe mitu korda külmas vees ja nõruta hästi. Pane karbi suurde potti koos 500ml veega. Kata kaanega, lase keema ja keeda 2 minutit, kuni karbi on avanenud. Vala poti sisu kurniga kausi kohale, et püüda karbi puljong. Kui piisavalt jahtunud, eemalda karbi koortest — jäta peotäis tühje kooreid serveerimiseks soovi korral. Sõelu karbi puljong kruusi, jättes põhja settinud liiva kaussi. Sul peaks olema umbes 800ml puljongit.

Kuumuta või samas potis ja prae peekonit 3-4 minutit kuni see hakkab pruunistuma. Lisa sibul, tüümian ja loorberileht ning hauta kõike õrnalt 10 minutit kuni sibul on pehme ja kuldne. Puista peale jahu ja sega, et tekiks liivane pasta, keeda 2 minutit, seejärel lisa järk-järgult karbi puljong, siis piim ja koor.

Lisa kartulid, lase kõik keema ja hauta õrnalt 10 minutit või kuni kartulid on küpsed. Purusta kahvliga mõned kartulitükid vastu poti külge, et supp pakseneks — aga osa tükke peaks ikka alles jääma. Sega sisse karbi liha ja mõned karbi koorikud kui kasutad neid, ning keeda minut et soojendada. Maitsesta rohke musta pipra ja vajadusel soolaga, seejärel sega sisse petersell vahetult enne kaussidesse ladumist või õõnestatud krõmpsuva saiasse serveerimist.""",

    '52844': """Kuumuta õli suures kastrulis. Kasuta köögikarusid et lõigata peekon väikesteks tükkideks, või kasuta teravat nuga lõikelaual. Lisa peekon pannile ja küpseta vaid mõni minut kuni hakkab kuldseks muutuma. Lisa sibul, seller ja porgand ning keeda keskmisel kuumusel 5 minutit, aeg-ajalt segades, kuni pehmed.

Lisa küüslauk ja küpseta 1 minut, seejärel lisa hakkliha ja küpseta, segades ning puust lusikaga purustades, umbes 6 minutit kuni igalt poolt pruunistunud.

Sega sisse tomatipasta ja küpseta 1 minut, segades hästi veise ja köögiviljadega. Lisa tükeldatud tomatid. Täida iga purk pooleldi veega, et loputada järelejäänud tomatid, ja lisa pannile. Lisa mesi ja maitsesta. Hauta 20 minutit.

Kuumuta ahi 200C/180C pöördõhk/gaas 6. Lasanje kokkupanemiseks tõsta kastmest natuke vormi põhja, laotades üle kogu pinna. Aseta 2 lasanjelehte kastme peale kattuvalt, seejärel korda kastme ja pasta kihtidega. Korda veel 2 kihiga kastet ja pastat, lõpetades pastakihiga.

Pane crème fraîche kaussi ja sega 2 sl veega, et see lahjeneks ja tekiks sile valatav kaste. Vala see pasta peale, seejärel kata mozzarellaga. Puista peale parmesan ja küpseta 25-30 minutit kuni kuldne ja mullitav. Serveeri basiilikuga üle puistatuna, kui soovid.""",

    '52874': """Kuumuta ahi 150C/300F/gaas 2.

SAMM 1
Sega veiseliha ja jahu kausis kokku ning maitsesta soola ja musta pipraga.

SAMM 2
Kuumuta suur pada, lisa pool rapsiõlist ja piisavalt veiseliha et katta paja põhi. Prae igalt poolt pruuniks, seejärel tõsta kõrvale. Korda ülejäänud õli ja lihaga.

SAMM 3
Pane liha tagasi patta, lisa vein ja keeda kuni vedeliku kogus on poole võrra vähenenud, seejärel lisa puljong, sibul, porgandid, tüümian ja sinep ning maitsesta hästi soola ja pipraga.

SAMM 4
Kata kaanega ja pane ahju kaheks tunniks.

SAMM 5
Võta ahjust välja, kontrolli maitset ja jäta jahtuma. Eemalda tüümian.

SAMM 6
Kui liha on jahtunud ja oled valmis piruka kokku panema, kuumuta ahi 200C/400F/gaas 6. Tõsta liha pirukavormi, pintselda ääred lahtiklopitud munakollastega ja aseta tainas peale. Pintselda taigna pind veel munakollasega.

SAMM 7
Trimmi tainas nii, et ääred saab krimpsu keerata, seejärel pane ahju ja küpseta 30 minutit, või kuni tainas on kuldpruun ja läbiküpsenud.

SAMM 8
Roheliste ubade jaoks lase soolaga maitsestatud vesi keema, lisa oad ja keeda 4-5 minutit, või kuni just pehmed. Nõruta ja sega võiga, maitsesta musta pipraga.

SAMM 9
Serveerimiseks tõsta igale taldrikule suur lusikatäis pirukat koos roheliste ubadega.""",

    '52917': """Kuumuta koor, šokolaad ja vaniljekaun pannil, kuni šokolaad on sulanud. Tõsta tulelt ja lase 10 minutit tõmmata, kraapides kauna seemned koore sisse. Kui kasutad vaniljeekstrakti, lisa kohe. Kuumuta ahi 160C/pöördõhk 140C/gaas 3.

Klopi munakollased ja suhkur heledaks. Sega sisse šokolaadikoor. Sõelu kruusi ja vala vormikestesse. Aseta sügavasse röstimispannile ja vala keev vesi poole kõrguseni. Küpseta 15-20 minutit kuni just tardunud kuid keskelt veel värisev. Jahuta külmkapis vähemalt 4 tundi.

Serveerimiseks puista brülee peale veidi suhkrut ja karamelliseeri leeklambi või lühidalt kuuma grilli all. Lase karamelil kõvastuda, seejärel serveeri.""",

    '52959': """Kuumuta ahi 180C/pöördõhk 160C/gaas 4. Lõika apteegitilli rohelised varred ära ja pane kõrvale. Lõika apteegitilli sibulad pooleks, seejärel iga pool 3 viiluks. Keeda soolaga maitsestatud vees 10 minutit, seejärel nõruta hästi. Haki apteegitilli rohelised varred jämedalt, sega peterselli ja sidrunikoorega.

Laota nõrutatud apteegitill madalasse ahjuvormi, lisa tomatid. Nirista peale oliiviõli, seejärel küpseta 10 minutit. Aseta lõhe köögiviljade vahele, nirista peale sidrunimahl, seejärel küpseta veel 15 minutit kuni kala on just küps. Puista peale petersell ja serveeri.""",

    '52982': """SAMM 1
Pane suur kastrul veega keema.

SAMM 2
Haki 100g pancettat peeneks, eemaldades enne kõik koor. Riivi peeneks 50g pecorino juustu ja 50g parmesani ning sega need omavahel.

SAMM 3
Klopi 3 suurt muna keskmises kausis lahti ja maitsesta värskelt jahvatatud musta pipraga. Pane kõik kõrvale.

SAMM 4
Lisa keevasse vette 1 tl soola, lisa 350g spagetid ja kui vesi uuesti keema läheb, keeda pidevalt podisedes kaane all 10 minutit või kuni al dente (just küps).

SAMM 5
Purusta noatera lameda küljega 2 puhastatud paksu küüslauguküünt, lihtsalt kergelt.

SAMM 6
Kuni spagetid keevad, prae pancettat küüslauguga. Pane 50g soolavaba võid suurde pannile või wokki ja niipea kui või on sulanud, lisa pancetta ja küüslauk.

SAMM 7
Prae keskmisel kuumusel umbes 5 minutit, sageli segades, kuni pancetta on kuldne ja krõbe. Küüslauk on oma maitse juba andnud — eemalda see lusikaga ja viska ära.

SAMM 8
Hoia pancetta all madalat kuumust. Kui pasta on valmis, tõsta see kahvli või tangidega veest välja ja pane pannile pancetta juurde. Ära muretse kui pisut vett pannile tilgub — see on soovitav — ja ära pasta keeduvett veel ära vala.

SAMM 9
Sega enamus juustust munade hulka, jättes väikese peotäie lõpus peale raputamiseks.

SAMM 10
Tõsta spagetipann pancettaga tulelt. Vala kiiresti sisse muna-juustu segu. Tangide või pika kahvliga tõsta spagetid üles, et need muna seguga hästi läbi seguneks — segu pakseneb aga ei kaldu kokku — ja kõik oleks kaetud.

SAMM 11
Lisa pasta keeduvett kastme hoidmiseks (mõni supilusikatäis piisab). Sa ei taha märga, vaid lihtsalt niisket. Maitsesta vajadusel soolaga.

SAMM 12
Keera pasta pika kahvliga serveerimistaldrikule või kaussi. Serveeri kohe, raputades peale ülejäänud juustu ja riivides musta pipart. Kui roog enne serveerimist pisut kuivaks jääb, lisa veidi kuuma pasta keeduvett ja läige tuleb tagasi.""",

    '53064': """Keeda pasta pakendil olevate juhiste järgi suures potis soolaga maitsestatud keevas vees.

Lisa vahukoor ja või suurele pannile keskmisele kuumusele kuni koor mullitab ja või sulab. Vahusta sisse parmesan ja maitsesta soola ja musta pipraga. Lase kastmel veidi pakseneda, seejärel lisa pasta ja sega kuni kastmega kaetud.

Kaunista peterselliga ja ongi valmis.""",

    '53065': """SAMM 1
SUSHIRULLIDE VALMISTAMISEKS: Laota riis laiali. Aseta nori-leht matile, läikiv pool allapoole. Kasta käed äädikavette, seejärel suru peotäied riisi peale 1cm paksuse kihina, jättes sinust kaugeima serva vabaks.

SAMM 2
Määri peale Jaapani majoneesi. Kasuta lusikat, et laotada õhuke kiht majoneesi riisi keskele.

SAMM 3
Lisa täidis. Lase lapsel lisada majoneesi peale rida oma lemmiktäidiseid — siin oleme kasutanud tuunikala ja kurki.

SAMM 4
Rulli kokku. Tõsta mati serv riisi kohale, vajutades kergelt et kõik tihedalt rulliks.

SAMM 5
Kleebi servad nagu postmark. Kui jõuad servani kus riisi pole, pintselda veidi veega ja jätka tihedaks rullimiseks.

SAMM 6
Mähi toidukilesse. Eemalda matt ja rulli tihedalt toidukilesse enne kui täiskasvanu lõikab sushi paksudeks viiludeks, seejärel eemalda toidukile.

SAMM 7
PRESSITUD SUSHI VALMISTAMISEKS: Lisa suitsulõhe kiht. Vooderda leivavorm toidukilega, seejärel aseta õhuke kiht suitsulõhet sisse toidukile peale.

SAMM 8
Kata riisiga ja vajuta alla. Suru umbes 3cm riisi kala peale, voldi toidukile üle ja vajuta nii palju kui saad, kasutades vajadusel teist vormi.

SAMM 9
Pööra välja nagu liivalossi. Pööra sushi plokk lõikelauale. Lase täiskasvanul lõigata sõrmedeks, seejärel eemalda toidukile.

SAMM 10
SUSHI PALLIDE VALMISTAMISEKS: Vali kate. Võta väike ruut toidukilest ja aseta peale kate, näiteks pool krevetti või väike tükk suitsulõhet. Kasuta niiskeid käsi, et rullida pähkli suurused riisipallid ja aseta katte peale.

SAMM 11
Tee tihedateks pallideks. Koguge toidukile nurgad kokku ja keera pallid tihedaks, seejärel lahti keera ja serveeri.""",

    '53261': """SAMM 1
Kasuta teravaid köögikarusid, et lõigata iga tiib liigesest kaheks tükiks. Sega küüslauk, sidrunikoor ja mahl, köömned ja õli rohke maitsestamisega, seejärel pane koos kanakoivadega kaussi ja sega katmiseks. Kata ja pane külmkappi marineerima vähemalt 1 tunniks, või üleöö kui aega on.

SAMM 2
Kuumuta ahi 200C/180C pöördõhk/gaas 6, või kuumuta välisgrill. Küpseta kanakoivad ahjuplaadil 45-50 minutit kuni krõbedad, või grilli 20 minutit, nirista mett peale viimase 10 minuti jooksul mõlemal meetodil. Serveeri vaagnal rohkete paberrätikutega. Täida väikesed kausid oliivide, pistaatsiapähklite või mandlite, datlite ja marineeritud tšillipaprikatega ning pita-leibadega serveerimiseks kõrvale.""",

    '53305': """SAMM 1
Kuumuta ahi 180C/pöördõhk 160C/gaas 4. Eemalda kapsalehtedest kõva keskmine vars. Lase suurel potil soolaga maitsestatud vesi keema, lisa kapsas, seejärel keeda vaid 1-2 minutit kuni lehed hakkavad närbuma. Nõruta ja jahuta külma jooksva vee all. Nõruta hästi, seejärel kuivata köögirätikuga.

SAMM 2
Kuumuta õli pannil, lisa sibul, seejärel prae 5 minutit kuni kergelt pruunistunud. Lisa rosmariin ja seller, seejärel küpseta veel 8 minutit. Sega sisse riis, seejärel keeda minut kuni terad läikivad. Tõsta tulelt, sega sisse kastanid ja jõhvikad, seejärel maitsesta.

SAMM 3
Tõsta veidi täidist kapsalehele, keera kokku ja voldi küljed sisse täidise sulgemiseks. Aseta ühe kihina suurde õlitatud madalasse ahjuvormi, liitekohaga allpool. Täida ülejäänud lehed samamoodi. Sega kokku puljong, äädikas ja mesi, seejärel vala kapsa peale. Kata vorm tihedalt fooliumiga, küpseta 1 tund, võta foolium ära, seejärel küpseta veel 15 minutit."""
}

updated = 0
for meal_id, instructions in translations.items():
    cur.execute(
        "UPDATE recipe_translations SET instructions_et = %s WHERE meal_id = %s",
        (instructions, meal_id)
    )
    updated += cur.rowcount
    print(f"✅ {meal_id}: updated {cur.rowcount} row")

conn.commit()
print(f"\nKokku uuendatud: {updated} retsepti")
cur.close()
conn.close()
