# Kenteken checker

Momentele staat: een redelijk snelle CSV parser en inserter

CSV's van de RDW zijn te vinden op: https://opendata.rdw.nl/Voertuigen/Open-Data-RDW-Gekentekende_voertuigen/m9d7-ebf2

Volle CSV te downloaden van: https://opendata.rdw.nl/resource/m9d7-ebf2.csv?$limit=99999999999999999999

TODO (non-exhaustive):
- [ ] CSV filename uit .env halen
- [ ] Automatisch downloaden van de CSV van de RDW en inlezen in de database (oude table renamen en nieuwe table aanmaken)
- [X] Optimaliseren DMV batch inserts en chunking van bestand
- [ ] API
- [ ] evt. recalls op auto's checken
