<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:oai="http://www.openarchives.org/OAI/2.0/"
        xmlns:oaire="http://namespace.openaire.eu/schema/oaire/"
        xmlns:oaf="http://namespace.openaire.eu/oaf"
        xmlns:datacite="http://datacite.org/schema/kernel-4"
        xmlns:dri="http://www.driver-repository.eu/namespace/dri"
        xmlns:xs="http://www.w3.org/2001/XMLSchema"
        xmlns:dr="http://www.driver-repository.eu/namespace/dr"
        xmlns:dc="http://purl.org/dc/elements/1.1/"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        version="2.0">

<xsl:param name="varAKA"    select = "'aka_________::'"/>
<xsl:param name="varARC"    select = "'arc_________::'"/>
<xsl:param name="varANR"    select = "'anr_________::'"/>
<xsl:param name="varCHISTERA" select = "'chistera____::'"/>
<xsl:param name="varCONICYT" select = "'conicytf____::'"/>
<xsl:param name="varDFG"    select = "'dfgf________::'"/>
<xsl:param name="varEUENVAGENCY" select = "'euenvagency_::'"/>
<xsl:param name="varFCT"    select = "'fct_________::'"/>
<xsl:param name="varFP7"    select = "'corda_______::'"/>
<xsl:param name="varFWF"    select = "'fwf_________::'"/>
<xsl:param name="varGSRI"   select = "'gsri________::'"/>
<xsl:param name="varGSRT"   select = "'gsrt________::'"/>
<xsl:param name="varH2020"  select = "'corda__h2020::'"/>
<xsl:param name="varHRZZ"   select = "'irb_hr______::'"/>    <!-- HRZZ not found -->
<xsl:param name="varINNOVIRIS" select = "'innoviris___::'"/>
<xsl:param name="varMESTD"  select = "'mestd_______::'"/>
<xsl:param name="varMIUR"   select = "'miur________::'"/>
<xsl:param name="varMZOS"   select = "'irb_hr______::'"/>
<xsl:param name="varNHMRC"  select = "'nhmrc_______::'"/>
<xsl:param name="varNIH"    select = "'nih_________::'"/>
<xsl:param name="varNSF"    select = "'nsf_________::'"/>
<xsl:param name="varNWO"    select = "'nwo_________::'"/>
<xsl:param name="varRCUK"   select = "'rcuk________::'"/>     <!-- RCUK getting changed to UKRI -->
<xsl:param name="varRIF"    select = "'rif_________::'"/>
<xsl:param name="varRSF"    select = "'rsf_________::'"/>
<xsl:param name="varSFI"    select = "'sfi_________::'"/>
<xsl:param name="varSFRS"   select = "'sfrs________::'"/>
<xsl:param name="varSGOV"   select = "'sgov________::'"/>     <!-- SGOV to be added, awaiting DOI from Pilar, found project ids not in CSV list? -->
<xsl:param name="varSNSF"   select = "'snsf________::'"/>
<xsl:param name="varTARA"   select = "'taraexp_____::'"/>     <!-- TARA to be added, awaiting DOI from André -->
<xsl:param name="varTUBITAK" select = "'tubitakf____::'"/>
<xsl:param name="varUKRI"   select = "'ukri________::'"/>     <!-- RCUK getting changed to UKRI -->
<xsl:param name="varWT"     select = "'wt__________::'"/>
