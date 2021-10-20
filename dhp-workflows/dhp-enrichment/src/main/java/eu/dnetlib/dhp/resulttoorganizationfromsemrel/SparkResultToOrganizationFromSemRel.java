package eu.dnetlib.dhp.resulttoorganizationfromsemrel;

public class SparkResultToOrganizationFromSemRel {

    //per ogni figlio nel file delle organizzazioni
    //devo fare una roba iterativa che legge info da un file e le cambia via via
    //passo 1: creo l'informazione iniale: organizzazioni che non hanno figli con almeno un padre
    //ogni organizzazione punta alla lista di padri
    //eseguo la propagazione dall'organizzazione figlio all'organizzazione padre
    //ricerco nel dataset delle relazioni se l'organizzazione a cui ho propagato ha, a sua volta, dei padri
    //e propago anche a quelli e cosi' via fino a che arrivo ad organizzazione senza padre

    //organizationFile:
    //f => {p1, p2, ..., pn}
    //resultFile
    //o => {r1, r2, ... rm}

    //supponiamo che f => {r1, r2} e che nessuno dei padri abbia gia' l'associazione con questi result
    //quindi
    //p1 => {r1, r2}
    //p2 => {r1, r2}
    //pn => {r1, r2}


    //mi serve un file con tutta la gerarchia per organizzazioni
    //un file con le organizzazioni foglia da joinare con l'altro file
    //un file con le associazioni organizzazione -> result forse meglio result -> organization

}
