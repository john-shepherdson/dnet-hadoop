<!-- adaptation 2021-08-23 -->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:oaf="http://namespace.openaire.eu/oaf"
                xmlns:dr="http://www.driver-repository.eu/namespace/dr"
                xmlns:datacite="http://datacite.org/schema/kernel-4"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:dri="http://www.driver-repository.eu/namespace/dri"
                xmlns:oaire="http://namespace.openaire.eu/schema/oaire/"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:oai="http://www.openarchives.org/OAI/2.0/"
                xmlns:TransformationFunction="eu.dnetlib.data.collective.transformation.core.xsl.ext.TransformationFunctionProxy"
                extension-element-prefixes="TransformationFunction"
                exclude-result-prefixes="TransformationFunction"
                version="1.1">
     
     <xsl:param name="varOfficialName" />
     <xsl:param name="varDsType" />
     <xsl:param name="varDataSourceId" />
     <xsl:param name="index" select="0"/>
     <xsl:param name="transDate" select="current-dateTime()"/>
     <xsl:variable name="tf" select="TransformationFunction:getInstance()"/>
     
     <xsl:param name="varFP7" select = "'corda_______::'"/>
     <xsl:param name="varH2020" select = "'corda__h2020::'"/>
     <xsl:param name="varAKA" select = "'aka_________::'"/>
     <xsl:param name="varARC" select = "'arc_________::'"/>
     <xsl:param name="varCONICYT" select = "'conicytf____::'"/>
     <xsl:param name="varDFG" select = "'dfgf________::'"/>
     <xsl:param name="varFCT" select = "'fct_________::'"/>
     <xsl:param name="varFWF" select = "'fwf_________::'"/>
     <xsl:param name="varHRZZ" select = "'irb_hr______::'"/>    <!-- HRZZ not found -->
     <xsl:param name="varMESTD" select = "'mestd_______::'"/>
     <xsl:param name="varMZOS" select = "'irb_hr______::'"/>
     <xsl:param name="varNHMRC" select = "'nhmrc_______::'"/>
     <xsl:param name="varNIH" select = "'nih_________::'"/>
     <xsl:param name="varNSF" select = "'nsf_________::'"/>
     <xsl:param name="varNWO" select = "'nwo_________::'"/>
     <xsl:param name="varRCUK" select = "'rcuk________::'"/>
     <xsl:param name="varSFI" select ="'sfi_________::'"/>
     <xsl:param name="varSGOV" select = "'sgov________::'"/>     <!-- SGOV to be added, awaiting DOI from Pilar, found project ids not in CSV list? -->
     <xsl:param name="varSNSF" select = "'snsf________::'"/>
     <xsl:param name="varTARA" select = "'taraexp_____::'"/>     <!-- TARA to be added, awaiting DOI from André -->
     <xsl:param name="varTUBITAK" select = "'tubitakf____::'"/>
     <xsl:param name="varWT" select = "'wt__________::'"/>
     
     <xsl:template match="/">
          <xsl:variable name="datasourcePrefix" select="normalize-space(//oaf:datasourceprefix)" />
          <xsl:if test="//oai:header/@status='deleted'">
               <xsl:call-template name="terminate"/>
          </xsl:if>
          <xsl:call-template name="validRecord" />
     </xsl:template>
     
     <xsl:template name="terminate">
          <xsl:message terminate="yes">
               record is not compliant, transformation is interrupted.
          </xsl:message>
     </xsl:template>
     
     <xsl:template name="validRecord">
          <record>
               <xsl:apply-templates select="//*[local-name() = 'header']" />
               <metadata>
                    
                    <!-- ~~~~~ pre-detect resourceType, superType and store in variable ~~~~~ -->
                    
                    <!-- optimize resourceType, superType in case of several statements (e.g. FU Berlin unfortunately contains record with several) -->
                    <!-- 
                         <xsl:variable name='varCobjCategory' select="TransformationFunction:convertString($tf, distinct-values(//*[local-name()='resourceType'][1]/@uri), 'TextTypologies')" />
                         <xsl:variable name="varSuperType" select="TransformationFunction:convertString($tf, $varCobjCategory, 'SuperTypes')"/>
                    -->
                    <xsl:variable name="varTypLst" select="distinct-values((//*[local-name()='resourceType']/(., @*)))"/>
                    <xsl:variable name="varCobjCatLst" select="distinct-values((for $i in $varTypLst      return TransformationFunction:convertString($tf, normalize-space($i), 'TextTypologies')))"/>
                    <xsl:variable name="varCobjSupLst" select="for $i in $varCobjCatLst      return concat($i, '###', TransformationFunction:convertString($tf, normalize-space($i), 'SuperTypes'))"/>
                    <xsl:variable name="varCobjSup" select="(
                                                            $varCobjSupLst[not(substring-after(., '###') = 'other') and not(substring-before(., '###') = ('0038', '0039', '0040'))][1], 
                                                            $varCobjSupLst[not(substring-after(., '###') = 'other')][1],
                                                            $varCobjSupLst[not(substring-before(., '###') = ('0020', '0000'))][1],
                                                            $varCobjSupLst[not(substring-before(., '###') = ('0000'))][1],
                                                            $varCobjSupLst[not(substring-before(., '###') = ('0000'))][1],
                                                            '0000')[1]"/>
                    <xsl:variable name="varSuperType" select="substring-after($varCobjSup, '###')"/>
                    <xsl:variable name="varCobjCategory" select="substring-before($varCobjSup, '###')"/>
                    
                    <!-- ~~~~~ pre-detect, -clean, -deduplicat PIDs and store in variable ~~~~~ -->
                    
                    <xsl:variable name="varKnownFileEndings" select="('.bmp', '.doc', '.docx', '.epub', '.flv', '.jpeg', '.jpg', '.m4v', '.mp4', '.mpg', '.odp', '.pdf', '.png', '.ppt', '.tiv', '.txt', '.xls', '.xlsx', '.zip')" />
                    
                    <!-- landingPage URL -->
                    <!-- either generate landing pages (as often not given), or compare URL with baseURL -->
                    <!-- 
                         // covered in comparison: URL encoded baseURLs, item URL und baseURL differing only in prefix www. or postfix port
                         // not covered in comparison: URL encoded item URL, URLs being a substring of URLs (like item URL http://def.br/... and baseURL http://abcdef.br/...), ports when only in baseURL or only in item URL
                         // tries to not consider higher level URLs as item URLs (e.g. journal URLs) by enforcing something after /
                    -->
                    <xsl:variable name="varOrigBaseUrl" select="//*[local-name() = 'about']/*[local-name() = 'provenance']//*[local-name() = 'originDescription' and not(./*[local-name() = 'originDescription'])]/*[local-name() = 'baseURL']" />
                    <xsl:variable name="varAggrBaseUrl" select="//*[local-name() = 'about']/*[local-name() = 'provenance']//*[local-name() = 'originDescription' and (./*[local-name() = 'originDescription'])]/*[local-name() = 'baseURL']" />
                    
                    <xsl:variable name="varLandingPage" select="(
                                                                //datacite:identifier[(contains(substring-after(., '://'), '/') and contains($varOrigBaseUrl, substring-before(substring-after(., '://'), '/'))) or (contains(substring-after(., '://'), ':') and contains($varOrigBaseUrl, substring-before(substring-after(., '://'), ':')))][not(replace(lower-case(.), '.*(\.[a-z]*)$', '$1') = $varKnownFileEndings)],
                                                                //datacite:identifier[(contains(substring-after(., '://'), '/') and contains($varAggrBaseUrl, substring-before(substring-after(., '://'), '/'))) or (contains(substring-after(., '://'), ':') and contains($varAggrBaseUrl, substring-before(substring-after(., '://'), ':')))][not(replace(lower-case(.), '.*(\.[a-z]*)$', '$1') = $varKnownFileEndings)],
                                                                //oaf:datasourceprefix[. = 'od_______268']/concat('https://qspace.library.queensu.ca/handle/', substring-after(//dri:recordIdentifier, 'oai:qspace.library.queensu.ca:')),
                                                                //oaf:datasourceprefix[. = 'od_______307']/concat('http://repositorium.sdum.uminho.pt/handle/', substring-after(//dri:recordIdentifier, 'oai:repositorium.sdum.uminho.pt:')),
                                                                //oaf:datasourceprefix[. = 'od_______317']/concat('https://papyrus.bib.umontreal.ca/xmlui/handle/', substring-after(//dri:recordIdentifier, 'oai:papyrus.bib.umontreal.ca:')),
                                                                //oaf:datasourceprefix[. = 'od______1318']/concat('https://orbi.uliege.be/handle/', substring-after(//dri:recordIdentifier, 'oai:orbi.ulg.ac.be:')),
                                                                //oaf:datasourceprefix[. = 'od______1388']/concat('http://rabida.uhu.es/dspace/handle/', substring-after(//dri:recordIdentifier, 'oai:rabida.uhu.es:')),
                                                                //oaf:datasourceprefix[. = 'od______1472']/concat('https://gredos.usal.es/handle/', substring-after(//dri:recordIdentifier, 'oai:gredos.usal.es:')),
                                                                //oaf:datasourceprefix[. = 'od______1560']/concat('https://riunet.upv.es/handle/', substring-after(//dri:recordIdentifier, 'oai:riunet.upv.es:')),
                                                                //oaf:datasourceprefix[. = 'od______3158']/concat('https://open.uct.ac.za/handle/', substring-after(//dri:recordIdentifier, 'oai:localhost:')),
                                                                //oaf:datasourceprefix[. = 'od______4225']/concat('https://repository.rothamsted.ac.uk/item/', substring-after(//dri:recordIdentifier, 'oai:repository.rothamsted.ac.uk:')),
                                                                //oaf:datasourceprefix[. = 'od______9626']/(//datacite:identifier)[@identifierType='Handle'][not(contains(., '/handle/123456789/'))],
                                                                //oaf:datasourceprefix[not(. = ('od_______268', 'od_______307', 'od______1318', 'od______1388', 'od______1472', 'od______4225'))]/(//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*) = ('url', 'uri')][not(contains(., 'doi.org/')) and not(contains(., '/doi/')) and not(contains(., '/pmc/'))][starts-with(., 'http')]
                                                                )[1]"/>
                    
                    <!-- IDs (identifier and alternateIdentifier, not relatedIdentifier) -->
                    <!-- container PIDs (ISSNs, ISBNs) often erroneously stuffed in alternateIdentifier for contained items (journal articles, book chapters, ...), are to be shifted into relatedIdentifier instead -->
                    <xsl:variable name="varPidDoi" select="concat(' DOI:::', string-join(distinct-values((
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[contains(., 'doi.org/10.')]/lower-case(substring-after(., 'doi.org/')),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[contains(., 'info:doi:')]/substring-after(., 'info:doi:'),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='doi'][not(contains(., 'doi.org/10.')) and not(contains(., 'info:doi:')) and not(contains(., '/doi/'))]/lower-case(.),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[matches(., '.*/doi/.*/10\..*')]/replace(., '.*/doi/.*/(10\..*)$', '$1')
                                                           )), ' DOI:::'))"/>
                    <xsl:variable name="varPidHandle" select="concat(' Handle:::', string-join(distinct-values((
                                                              (//datacite:identifier, //datacite:alternateIdentifier)[contains(., 'hdl.handle.net/')][not(contains(., '123456789'))]/substring-after(., 'hdl.handle.net/'),
                                                              (//datacite:identifier, //datacite:alternateIdentifier)[contains(., 'info:hdl:')][not(contains(., '123456789'))]/substring-after(., 'info:hdl:'),
                                                              (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='handle'][not(starts-with(., 'http')) and not(contains(., 'hdl.handle.net/')) and not(contains(., 'info:hdl:')) and not(contains(., '/handle/123456789/'))]
                                                              )), ' Handle:::'))"/>
                    <xsl:variable name="varPidUrn" select="concat(' URN:::', string-join(distinct-values((
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'urn:nbn:')]/concat('urn:nbn:', substring-after(lower-case(.), 'urn:nbn:')),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'http') and (contains(lower-case(.), '://nbn-resolving.org/urn:nbn:') or contains(lower-case(.), '://nbn-resolving.de/urn/resolver.pl?urn:nbn:') or contains(lower-case(.), '://nbn-resolving.de/urn:nbn:') or contains(lower-case(.), '://resolver.obvsg.at/urn:nbn:') or contains(lower-case(.), '://urn.fi/urn:nbn:') or contains(lower-case(.), '://urn.kb.se/resolve?urn=urn:nbn:'))]/concat('urn:nbn:', substring-after(lower-case(.), 'urn:nbn:'))
                                                           )), ' URN:::'))"/>
                    <xsl:variable name="varPidArk" select="concat(' ARK:::', string-join(distinct-values((
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(., 'http') and contains(., '/ark:/')]/concat('ark:/', substring-after(., '/ark:/'))
                                                           )), ' ARK:::'))"/>
                    <xsl:variable name="varPidPmid" select="concat(' PMID:::', string-join(distinct-values((
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(., 'http') and contains(., '://www.ncbi.nlm.nih.gov/pmc/articles/')]/substring-after(., '://www.ncbi.nlm.nih.gov/pmc/articles/'),
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(., 'info:pmid/')]/substring-after(., 'info:pmid/'),
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'pmid:') or starts-with(lower-case(.), 'pubmed:')]/substring-after(., ':'),
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='pmc'][not(contains(., 'ncbi.nlm.nih.gov'))][not(contains(., ':'))],
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='pmid'][not(contains(., ':'))]
                                                            )), ' PMID:::'))"/>
                    <xsl:variable name="varPidPmcid" select="concat(' PMCID:::', string-join(distinct-values((
                                                             (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(., 'http') and (contains(lower-case(.), '://www.ncbi.nlm.nih.gov/pmc/articles/pmc') or contains(lower-case(.), '://europepmc.org/articles/pmc'))]/substring-after(., '/articles/'),
                                                             (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'pmcid:') or starts-with(lower-case(.), 'pmc:')]/substring-after(., ':'),
                                                             (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='pmcid']
                                                             )), ' PMCID:::'))"/>
                    <xsl:variable name="varPidHal" select="concat(' HAL:::', string-join(distinct-values((
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='hal' and (starts-with(., 'hal-') or starts-with(., 'halshs-') or starts-with(., 'halsde-'))],
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(., 'http') and (contains(., '://hal.archives-ouvertes.fr/hal') or contains(., '://hal.inria.fr/hal') or contains(., '://halshs.archives-ouvertes.fr/hal') or contains(., '://halsde.archives-ouvertes.fr/hal'))]/substring-after(., '.fr/')
                                                           )), ' HAL:::'))"/>
                    <xsl:variable name="varPidBibc" select="concat(' BIBC:::', string-join(distinct-values((
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'bibcode:')]/substring-after(lower-case(.), 'bibcode:'),
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(., 'http') and contains(lower-case(.), 'bibcode=')]/substring(substring-after(lower-case(.), 'bibcode='), 1, 19)
                                                            )), ' BIBC:::'))"/>
                    <xsl:variable name="varPidArxiv" select="concat(' arXiv:::', string-join(distinct-values((
                                                             (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'arxiv:')]/substring-after(lower-case(.), 'arxiv:'),
                                                             (//datacite:identifier, //datacite:alternateIdentifier)[(starts-with(., 'http') or starts-with(lower-case(.), 'arxiv: http')) and contains(., '://arxiv.org/abs/')]/substring-after(., '://arxiv.org/abs/'),
                                                             (//datacite:identifier, //datacite:alternateIdentifier)[(starts-with(., 'http') or starts-with(lower-case(.), 'arxiv: http')) and contains(., '://arxiv.org/pdf/')]/substring-before(substring-after(lower-case(.), '://arxiv.org/pdf/'), '.pdf'),
                                                             (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='arxiv']
                                                             )), ' arXiv:::'))"/>
                    <xsl:variable name="varPidWos" select="concat(' WOS:::', string-join(distinct-values((
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='wos'][not(starts-with(lower-case(.), 'wos:'))],
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'wos:') and not(starts-with(lower-case(.), 'wos: wos:'))]/substring-after(lower-case(.), 'wos:'),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'wos: wos:')]/substring-after(lower-case(.), 'wos: wos:')
                                                           )), ' WOS:::'))"/>
                    <xsl:variable name="varPidScp" select="concat(' SCP:::', string-join(distinct-values((
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(lower-case(.), 'scopus_id:') or starts-with(lower-case(.), 'scopus:')]/substring-after(., ':'),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(normalize-space(.), 'scopus: eid=')]/substring-after(., 'eid=2-s2.0-'),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(normalize-space(.), 'http') and contains(., '://www.scopus.com/inward/record.ur') and contains(., 'scp=')]/substring-after(., 'scp='),
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[starts-with(normalize-space(.), 'http') and contains(., '://www.scopus.com/inward/record.ur') and contains(., 'eid=2-s2.0-')]/substring-after(., 'eid=2-s2.0-')
                                                           )), ' SCP:::'))"/>
                    <xsl:variable name="varPidUrl" select="concat(' URL:::', string-join(distinct-values((
                                                           (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='url']
                                                           [not(contains(., 'doi.org/10.')) and not(contains(., 'info:doi:')) and not(matches(., '.*/doi/.*/10\..*'))]
                                                           [not(contains(., 'hdl.handle.net/')) and not(contains(., 'info:hdl:'))]
                                                           [not(contains(., '://www.ncbi.nlm.nih.gov/pmc/articles/'))]
                                                           [not(. = $varLandingPage)]
                                                           )), ' URL:::'))"/>
                    <xsl:variable name="varPidIsbn" select="concat(' ISBN:::', string-join(distinct-values((
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='isbn' and not(contains(., ':')) and not($varCobjCategory = '0013')],
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)='isbn' and starts-with(lower-case(.), 'urn:isbn') and not($varCobjCategory = '0013')]/substring-after(lower-case(.), 'urn:isbn:')
                                                            )), ' ISBN:::'))"/>
                    <xsl:variable name="varPidIssn" select="string-join(distinct-values((
                                                            (//datacite:identifier, //datacite:alternateIdentifier)[lower-case(@*)=('issn', 'pissn', 'eissn', 'lissn') and matches(., '.*\d{4}[\s-]?\d{3}[\dXx].*') and not($varCobjCategory = '0001')]/concat(' ', upper-case((@identifierType, @alternateIdentifierType)), ':::', replace(., '^.*(\d{4})[\s-]?(\d{3}[\dXx]).*$', '$1-$2'))
                                                            )), '')"/>
                    
                    <!-- heap all IDs, drop landingPage URL -->
                    <xsl:variable name="varPidPre" select="concat($varPidDoi[not(. = ' DOI:::')], $varPidHandle[not(. = ' Handle:::')], $varPidUrn[not(. = ' URN:::')], $varPidArk[not(. = ' ARK:::')], $varPidPmid[not(. = ' PMID:::')], $varPidPmcid[not(. = ' PMCID:::')], $varPidHal[not(. = ' HAL:::')], $varPidBibc[not(. = ' BIBC:::')], $varPidArxiv[not(. = ' arXiv:::')], $varPidWos[not(. = ' WOS:::')], $varPidScp[not(. = ' SCP:::')], $varPidUrl[not(. = ' URL:::')], $varPidIsbn[not(. = ' ISBN:::')], $varPidIssn[not(. = ' ISSN:::')])"/>
                    
                    <!--
                         <xsl:variable name="varPid" select="concat($varPidPre, $varLandingPage[string-length($varPidPre) = 0]/concat('landingPage:::', .))"/>
                         <xsl:variable name="varPid" select="concat($varPidPre, concat(' landingPage:::', $varLandingPage[string-length($varPidPre) = 0], .))"/>
                    -->
                    <xsl:variable name="varPid" select="concat($varPidPre, //oaf:datasourceprefix[string-length($varPidPre) = 0]/concat(' landingPage:::', $varLandingPage))"/>
                    
                    <!-- relatedIdentifier -->
                    <!-- container PIDs (ISSNs, ISBNs) often erroneously stuffed in alternateIdentifier for contained items (journal articles, book chapters, ...), are to be shifted into relatedIdentifier instead -->
                    <xsl:variable name="varRelPidDoi" select="concat(' DOI:::', string-join(distinct-values((
                                                              (//datacite:relatedIdentifier)[contains(., 'doi.org/10.')]/concat(lower-case(substring-after(., 'doi.org/')), ':::', @relationType),
                                                              (//datacite:relatedIdentifier)[contains(., 'info:doi:')]/concat(substring-after(., 'info:doi:'), ':::', @relationType),
                                                              (//datacite:relatedIdentifier)[@relatedIdentifierType='DOI'][not(contains(., 'doi.org/10.')) and not(contains(., 'info:doi:')) and not(contains(., '/doi/'))]/concat(lower-case(.), ':::', @relationType),
                                                              (//datacite:relatedIdentifier)[matches(., '.*/doi/.*/10\..*')]/concat(replace(., '.*/doi/.*/(10\..*)$', '$1'), ':::', @relationType)
                                                              )), ' DOI:::'))"/>
                    <xsl:variable name="varRelPidHandle" select="concat(' Handle:::', string-join(distinct-values((
                                                                 (//datacite:relatedIdentifier)[contains(., 'hdl.handle.net/')]/concat(substring-after(., 'hdl.handle.net/'), ':::', @relationType),
                                                                 (//datacite:relatedIdentifier)[contains(., 'info:hdl:')]/concat(substring-after(., 'info:hdl:'), ':::', @relationType),
                                                                 (//datacite:relatedIdentifier)[lower-case(@relatedIdentifierType)='handle'][not(contains(., 'hdl.handle.net/')) and not(contains(., 'info:hdl:'))]/concat(., ':::', @relationType)
                                                                 )), ' Handle:::'))"/>
                    <xsl:variable name="varRelPidPmid" select="concat(' PMID:::', string-join(distinct-values((
                                                               (//datacite:relatedIdentifier)[contains(., '://www.ncbi.nlm.nih.gov/pmc/articles/')]/concat(substring-after(., '://www.ncbi.nlm.nih.gov/pmc/articles/'), ':::', @relationType),
                                                               (//datacite:relatedIdentifier)[lower-case(@*)='pmc'][not(contains(., 'ncbi.nlm.nih.gov'))]/concat(., ':::', @relationType)
                                                               )), ' PMID:::'))"/>
                    <xsl:variable name="varRelPidPmcid" select="concat(' PMCID:::', string-join(distinct-values((
                                                                (//datacite:relatedIdentifier)[lower-case(@*)='pmcid']
                                                                /concat(., ':::', @relationType)
                                                                )), ' PMCID:::'))"/>
                    <xsl:variable name="varRelPidArxiv" select="concat(' arXiv:::', string-join(distinct-values((
                                                                (//datacite:relatedIdentifier)[lower-case(@*)='arxiv']
                                                                /concat(., ':::', @relationType)
                                                                )), ' arXiv:::'))"/>
                    <xsl:variable name="varRelPidWos" select="concat(' WOS:::', string-join(distinct-values((
                                                              (//datacite:relatedIdentifier)[lower-case(@*)='wos'][not(starts-with(lower-case(.), 'wos:'))]/concat(., ':::', @relationType),
                                                              (//datacite:relatedIdentifier)[starts-with(lower-case(.), 'wos:')]/concat(substring-after(lower-case(.), 'wos:'), ':::', @relationType)
                                                              )), ' WOS:::'))"/>
                    <xsl:variable name="varRelPidUrl" select="concat(' URL:::', string-join(distinct-values((
                                                              (//datacite:relatedIdentifier)[lower-case(@*)='url']
                                                              [not(contains(., 'doi.org/10.')) and not(contains(., 'info:doi:')) and not(matches(., '.*/doi/.*/10\..*'))]
                                                              [not(contains(., 'hdl.handle.net/')) and not(contains(., 'info:hdl:'))]
                                                              [not(contains(., '://www.ncbi.nlm.nih.gov/pmc/articles/'))]
                                                              /concat(., ':::', @relationType)
                                                              )), ' URL:::'))"/>
                    
                    <xsl:variable name="varRelPid" select="concat($varRelPidDoi[not(. = ' DOI:::')], $varRelPidHandle[not(. = ' Handle:::')], $varRelPidPmid[not(. = ' PMID:::')], $varRelPidPmcid[not(. = ' PMCID:::')], $varRelPidArxiv[not(. = ' arXiv:::')], $varRelPidWos[not(. = ' WOS:::')], $varRelPidUrl[not(. = ' URL:::')])"/>
                    
                    <!-- container PIDs -->
                    <xsl:variable name="varContainerPidIssn" select="string-join(distinct-values((
                                                                     (//datacite:alternateIdentifier, //datacite:identifier)[$varCobjCategory = '0001' and lower-case(@*) = ('issn', 'pissn', 'eissn', 'lissn')]/concat(' ', @*[local-name() = ('identifierType', 'alternateIdentifierType')]/upper-case(.), ':::', substring(normalize-space(.), 1, 4), '-', substring(normalize-space(.), string-length(normalize-space(replace(., '\(.*$', '')))-3, 4), ':::0043'),
                                                                     (//datacite:relatedIdentifier)[lower-case(@*) = ('issn', 'pissn', 'eissn', 'lissn') and lower-case(@relationType) = 'ispartof' and not(contains(lower-case(.), 'doi')) and matches(., '.*\d{4}[\-\s]*\d{3}[\dXx].*') and not(contains(lower-case(.), 'edition'))]/concat(' ', upper-case(@relatedIdentifierType), ':::', substring(normalize-space(replace(., 'ISSN:?', '')), 1, 4), '-', substring(normalize-space(.), string-length(normalize-space(replace(., '\(.*$', '')))-3, 4), (.[$varCobjCategory = '0001' or //*[local-name()='resourceType' and contains(@uri, 'c_dcae04bc')]]/':::0043', .[$varCobjCategory = '0013']/'0002', //*[local-name()='resourceType'][./@uri=('http://purl.org/coar/resource_type/c_5794', 'http://purl.org/coar/resource_type/c_6670')]/'0004')[1])
                                                                     )), '')"/>
                    <xsl:variable name="varContainerPidIsbn" select="string-join(distinct-values((
                                                                     (//datacite:alternateIdentifier, //datacite:identifier)[$varCobjCategory = '0013' and lower-case(@*) = 'isbn'][not(contains(., ':'))][not(contains(normalize-space(.), ' '))]/concat(' ', @*[local-name() = ('identifierType', 'alternateIdentifierType')]/upper-case(.), ':::', normalize-space(.), ':::0002'),
                                                                     (//datacite:alternateIdentifier, //datacite:identifier)[$varCobjCategory = '0013' and starts-with(lower-case(.), 'urn:isbn:')][not(contains(normalize-space(.), ' '))]/concat(' ', @*[local-name() = ('identifierType', 'alternateIdentifierType')]/upper-case(.), ':::', substring-after(normalize-space(.), 'urn:isbn:'), ':::0002'),
                                                                     (//datacite:relatedIdentifier)[lower-case(@*) = 'isbn' and lower-case(./@relationType) = 'ispartof']/concat(' ', upper-case(@relatedIdentifierType), ':::', normalize-space(.), ':::', (.[$varCobjCategory = '0001']/':::0043', .[$varCobjCategory = '0013']/'0002', //*[local-name()='resourceType'][./@uri=('http://purl.org/coar/resource_type/c_5794', 'http://purl.org/coar/resource_type/c_6670')]/'0004')[1])
                                                                     )), '')"/>
                    <xsl:variable name="varContainerPidUrl" select="string-join(distinct-values((
                                                                    (//datacite:relatedIdentifier)[lower-case(@*) = 'url' and lower-case(./@relationType) = 'ispartof'][not(contains(normalize-space(.), ' ')) and not(contains(., 'doi/'))]/concat(' ', upper-case(@relatedIdentifierType), ':::', normalize-space(.), ':::', (.[$varCobjCategory = ('0024', '0025', '0030', '0033')]/':::0022')[1])
                                                                    )), '')"/>
                    
                    <xsl:variable name="varContainerPid" select="concat($varContainerPidIssn[not(. = ' ISSN:::')], $varContainerPidIsbn[not(. = ' ISBN:::')], $varContainerPidUrl[not(. = ' URL:::')])"/>
                    
                    <!-- drop oaire resource -->
                    <!-- <oaire:resource xmlns="http://namespace.openaire.eu/schema/oaire/"> -->
                    <datacite:resource>
                         
                         <!-- ~~~~~ IDs ~~~~~ -->
                         
                         <!-- choose 'best' PID among identifiers and alternateIdentifers, and cut off DOI resolver prefix to just get the number part -->
                         <datacite:identifier>
                              <xsl:attribute name="identifierType" select="substring-before(tokenize(normalize-space($varPid), ' ')[1], ':::')"/>
                              <xsl:value-of select="substring-after(tokenize(normalize-space($varPid), ' ')[1], ':::')"/>
                         </datacite:identifier>
                         
                         <datacite:alternateIdentifiers>
                              <xsl:for-each select="tokenize(normalize-space($varPid), ' ')[position() > 1][string-length(substring-after(., ':::')) > 0]">
                                   <datacite:alternateIdentifier>
                                        <xsl:attribute name="alternateIdentifierType" select="substring-before(., ':::')"/>
                                        <xsl:value-of select="substring-after(., ':::')" />
                                   </datacite:alternateIdentifier>
                              </xsl:for-each>
                              <xsl:for-each select="(//datacite:alternateIdentifier, //datacite:identifier)
                                                    [not($varCobjCategory = '0001' and ./@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = ('issn', 'pissn', 'eissn', 'lissn'))]
                                                    [not($varCobjCategory = '0013' and ./@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = 'isbn')]
                                                    [not(ends-with(., 'pdf'))]
                                                    [not(@*[local-name()=('identifierType', 'alternateIdentifierType')]/lower-case(.) = ('doi', 'handle', 'urn', 'pmid', 'pmcid', 'arxiv', 'wos', 'url', 'isbn', 'issn', 'eissn', 'lissn', 'pissn'))]
                                                    [not(. = $varLandingPage)]">
                                   <datacite:alternateIdentifier>
                                        <xsl:attribute name="alternateIdentifierType" select="./@*[local-name()=('identifierType', 'alternateIdentifierType')]"/>
                                        <xsl:value-of select="."/>
                                   </datacite:alternateIdentifier>
                              </xsl:for-each>
                              <xsl:if test="not(starts-with(normalize-space(lower-case($varPid)), 'landingpage')) and string-length($varLandingPage) > 0">
                                   <datacite:alternateIdentifier>
                                        <xsl:attribute name="alternateIdentifierType" select="'landingPage'"/>
                                        <xsl:value-of select="$varLandingPage"/>
                                   </datacite:alternateIdentifier>
                              </xsl:if>
                         </datacite:alternateIdentifiers>
                         
                         <datacite:relatedIdentifiers>
                              <xsl:for-each select="tokenize(normalize-space($varRelPid), ' ')
                                                    [not(contains(lower-case($varContainerPid), lower-case(substring-before(substring-after(., ':::'), ':::'))))]">
                                   <datacite:relatedIdentifier>
                                        <xsl:attribute name="relatedIdentifierType" select="substring-before(., ':::')"/>
                                        <xsl:attribute name="relationType" select="substring-after(substring-after(., ':::'), ':::')"/>
                                        <xsl:value-of select="substring-before(substring-after(., ':::'), ':::')" />
                                   </datacite:relatedIdentifier>
                              </xsl:for-each>
                              <xsl:for-each select="(//datacite:relatedIdentifier)
                                                    [not(@relatedIdentifierType/lower-case(.) = ('doi', 'handle', 'pmid', 'pmcid', 'wos', 'url'))]
                                                    [not(starts-with(lower-case(.), 'wos:') or contains(., 'hdl.handle.net/') or contains(., 'doi.org/10.'))]
                                                    [not(@relatedIdentifierType/lower-case(.) = ('issn', 'pissn', 'eissn', 'lissn', 'isbn') and @relationType/lower-case(.) = 'ispartof')]">
                                   <datacite:relatedIdentifier>
                                        <xsl:attribute name="relatedIdentifierType" select="./@relatedIdentifierType"/>
                                        <xsl:attribute name="relationType" select="./@relationType"/>
                                        <xsl:value-of select=".[not(@relatedIdentifierType/lower-case(.) = ('issn', 'pissn', 'eissn', 'lissn'))],
                                                              .[@relatedIdentifierType/lower-case(.) = ('issn', 'pissn', 'eissn', 'lissn')]/concat(substring(., 1, 4), '-', substring(., string-length(.)-3, 4))"/>
                                   </datacite:relatedIdentifier>
                              </xsl:for-each>
                              <xsl:for-each select="tokenize(normalize-space($varContainerPid), ' ')">
                                   <datacite:relatedIdentifier>
                                        <xsl:attribute name="relatedIdentifierType" select="substring-before(., ':::')"/>
                                        <xsl:attribute name="relationType" select="'IsPartOf'"/>
                                        <xsl:value-of select="substring-before(substring-after(., ':::'), ':::')" />
                                        <!-- <xsl:value-of select="substring-after(., ':::')" /> -->
                                   </datacite:relatedIdentifier>
                              </xsl:for-each>
                         </datacite:relatedIdentifiers>
                         
                         <!-- change namespace/format from oaire to datacite for description, language, rights, ... -->
                         
                         <xsl:for-each select="//oaire:resourceType">
                              <datacite:resourceType>
                                   <xsl:attribute name="xs:anyURI" select="@uri"/>
                                   <xsl:value-of select="."/>
                              </datacite:resourceType>
                         </xsl:for-each>
                         
                         <xsl:for-each select="distinct-values(//oaire:version)">
                              <datacite:version>
                                   <xsl:value-of select="."/>
                              </datacite:version>
                         </xsl:for-each>
                         
                         <xsl:if test="(//datacite:rights, //oaire:licenseCondition)[string-length(.) > 0 or string-length(@rightsURI) > 0 or string-length(@uri) > 0]">
                              <datacite:rightsList>
                                   <xsl:for-each select="(//datacite:rights | //oaire:licenseCondition)[string-length(.) > 0 or string-length(@rightsURI) > 0 or string-length(@uri) > 0]">
                                        <datacite:rights>
                                             <xsl:attribute name="rightsURI" select="(@rightsURI, @uri)[1]"/>
                                             <xsl:value-of select="."/>
                                        </datacite:rights>
                                   </xsl:for-each>
                              </datacite:rightsList>
                         </xsl:if>
                         
                         <xsl:if test="//datacite:title">
                              <datacite:titles>
                                   <xsl:for-each select="//datacite:title">
                                        <datacite:title>
                                             <xsl:for-each select="@*">
                                                  <xsl:copy/>
                                             </xsl:for-each>
                                             <xsl:value-of select="."/>
                                        </datacite:title>
                                   </xsl:for-each>
                              </datacite:titles>
                         </xsl:if>
                         
                         <xsl:if test="//dc:description">
                              <datacite:descriptions>
                                   <xsl:for-each select="//dc:description">
                                        <datacite:description>
                                             <xsl:attribute name="descriptionType" select="'Abstract'"/>
                                             <xsl:for-each select="@*">
                                                  <xsl:copy/>
                                             </xsl:for-each>
                                             <xsl:value-of select="."/>
                                        </datacite:description>
                                   </xsl:for-each>
                              </datacite:descriptions>
                         </xsl:if>
                         
                         <xsl:for-each select="distinct-values(//dc:language)[string-length(normalize-space(.)) > 0]">
                              <datacite:language>
                                   <xsl:value-of select="normalize-space(.)"/>
                              </datacite:language>
                         </xsl:for-each>
                         
                         <xsl:for-each select="distinct-values(//dc:publisher)[string-length(normalize-space(.)) > 0]">
                              <datacite:publisher>
                                   <xsl:value-of select="normalize-space(.)"/>
                              </datacite:publisher>
                         </xsl:for-each>
                         
                         <xsl:if test="//dc:format[string-length(normalize-space(.)) > 0]">
                              <datacite:formats>
                                   <xsl:for-each select="distinct-values(//dc:format)[string-length(normalize-space(.)) > 0]">
                                        <datacite:format>
                                             <xsl:value-of select="normalize-space(.)"/>
                                        </datacite:format>
                                   </xsl:for-each>
                              </datacite:formats>
                         </xsl:if>
                         
                         <xsl:if test="//oaire:fundingReference[./oaire:awardNumber]">
                              <datacite:fundingReferences>
                                   <xsl:for-each select="//oaire:fundingReference[./oaire:awardNumber]">
                                        <datacite:fundingReference>
                                             <datacite:funderName>
                                                  <xsl:value-of select="./oaire:funderName"/>
                                             </datacite:funderName>                
                                             <datacite:funderIdentifier>
                                                  <xsl:attribute name="funderIdentifierType" select="'Crossref Funder ID'"/>
                                                  <xsl:value-of select="./oaire:funderIdentifier"/>
                                             </datacite:funderIdentifier>
                                             <datacite:awardNumber>
                                                  <xsl:value-of select="./oaire:awardNumber"/>
                                             </datacite:awardNumber>
                                             <!--<xsl:value-of select="."/>-->
                                        </datacite:fundingReference>
                                   </xsl:for-each>
                              </datacite:fundingReferences>
                         </xsl:if>
                         
                         <xsl:apply-templates select="(//*[local-name()='resource'], //*[local-name() = 'oai_openaire'])/*[not(local-name() = ('identifier', 'alternateIdentifiers', 'alternateIdentifier', 'relatedIdentifiers', 'relatedIdentifier', 'description', 'titles', 'title', 'language', 'publisher', 'resourceType', 'version', 'fundingReferences', 'fundingReference', 'rights', 'licenseCondition', 'file', 'format', 'audience', 'source', 'coverage'))][not(starts-with(local-name(), 'citation'))]"/>
                         
                         <!-- </oaire:resource> -->
                    </datacite:resource>
                    
                    <!-- ~~~~~ put oaf elements below datacite:resource ~~~~~ -->
                    
                    <!-- oaf:identifier -->
                    <xsl:for-each select="tokenize(normalize-space($varPid), ' ')[string-length(substring-after(normalize-space(.), ':::')) > 0]">
                         <oaf:identifier>
                              <xsl:attribute name="identifierType" select="substring-before(normalize-space(.), ':::')"/>
                              <xsl:value-of select="substring-after(normalize-space(.), ':::')"/>
                         </oaf:identifier>
                    </xsl:for-each>
                    <xsl:if test="not(starts-with(normalize-space(lower-case($varPid)), 'landingpage')) and string-length($varLandingPage) > 0">
                         <oaf:identifier>
                              <xsl:attribute name="identifierType" select="'landingPage'"/>
                              <xsl:value-of select="$varLandingPage"/>
                         </oaf:identifier>
                    </xsl:if>
                    <xsl:if test="//*[local-name() = 'about']/*[local-name() = 'provenance']/*[local-name() = 'originDescription']/*[local-name() = 'originDescription']/*[local-name() = 'identifier' and string-length(.) > 0]">
                         <oaf:identifier>
                              <xsl:attribute name="identifierType" select="'oai-original'"/>
                              <xsl:value-of select="//*[local-name() = 'about']/*[local-name() = 'provenance']//*[local-name() = 'originDescription' and not(./*[local-name() = 'originDescription'])]/*[local-name() = 'identifier']"/>
                         </oaf:identifier>
                    </xsl:if>
                    
                    <xsl:variable name='varEmbargoEndDate' select="TransformationFunction:convertString($tf, normalize-space(//*[local-name()='date'][@dateType='Available']), 'DateISO8601')"/>
                    
                    <!-- resourceType, superType -->
                    <xsl:choose>
                         <xsl:when test="lower-case(//*[local-name()='resourceType']/@resourceTypeGeneral) = ('dataset', 'software', 'literature', 'publication', 'other research product') or not(//*[local-name()='resourceType']/@resourceTypeGeneral)">
                              <dr:CobjCategory>
                                   <xsl:attribute name="type" select="//oaf:datasourceprefix[. = '_______qeios' and contains(//dri:recordIdentifier, '/definition/')]/'other', //oaf:datasourceprefix[not(. = '_______qeios' and contains(//dri:recordIdentifier, '/definition/'))]/$varSuperType"/>
                                   <xsl:value-of select="$varCobjCategory" />
                              </dr:CobjCategory>
                         </xsl:when>
                         <!-- drop journals -->
                         <xsl:when test="lower-case(//*[local-name()='resourceType']/@uri) = 'http://purl.org/coar/resource_type/c_0640'">
                              <xsl:call-template name="terminate"/>
                         </xsl:when>
                         <xsl:otherwise>
                              <xsl:call-template name="terminate"/>
                         </xsl:otherwise>
                    </xsl:choose>
                    
                    <!-- review status -->
                    <xsl:variable name="varRefereedConvt" select="for $i in (
                                                                  //*[local-name()='resourceType']/(., @uri), //oai:setSpec, //*[local-name()='description']) 
                                                                  return TransformationFunction:convertString($tf, normalize-space($i), 'ReviewLevels')"/>
                    <xsl:variable name="varRefereedIdntf" select="(
                                                                  //*[local-name()=('identifier', 'alternateIdentifier', 'file')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier', 'file')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])pre[\.\-_\\/\s\(\)%\d#:]?prints?([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0002', 
                                                                  //*[local-name()=('identifier', 'alternateIdentifier', 'file')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier', 'file')]) = 1][matches(lower-case(.), '(^|.*[\.\-_\\/\s\(\)%\d#:])refereed([\.\-_\\/\s\(\)%\d#:].*)?$')]/'0001',
                                                                  //*[local-name()=('identifier', 'alternateIdentifier', 'file')][count(//*[local-name()=('metadata', 'resource')]//*[local-name()=('identifier', 'alternateIdentifier', 'file')]) = 1][matches(lower-case(.), '.*-peer-reviewed-(fulltext-)?article-.*')]/'0001')"/>
                    <xsl:variable name="varRefereedSourc" select="(//*[local-name()=('publisher', 'source', 'citationTitle')][matches(lower-case(.), '.*[\s\-\.\\_/:%]pre[\s\-\.\\_/:%]?prints?([\s\-\.\\_/:%].*|$)')]/'0002')"/>
                    <xsl:variable name="varRefereedReltn" select="//*[local-name() = 'relatedIdentifier'][./@relationType/lower-case(.)='isreviewedby']/'0001'"/>
                    <xsl:variable name="varRefereedDesct" select="(//*[local-name() = 'description']
                                                                  [matches(lower-case(.), '.*(this\s*book|this\s*volume|it)\s*(constitutes|presents)\s*the\s*(thoroughly\s*)?refereed') or 
                                                                  matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or 
                                                                  matches(lower-case(.), '(this|a)\s*(article|preprint)\s*(has\s*been\s*)?(peer[\-\s]*)?reviewed\s*and\s*recommended\s*by\s*peer[\-\s]*community')]/'0001')"/>
                    <xsl:variable name="varRefereedTitle" select="(//*[local-name()=('title')][matches(lower-case(.), '.*\[.*peer[\s\-\._]*review\s*:.*\]\s*$')]/'0001')"/>
                    <xsl:variable name="varRefereedVersn" select="TransformationFunction:convertString($tf, normalize-space( //*[local-name()='version']), 'ReviewLevels')"/>
                    <xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedIdntf, $varRefereedSourc, $varRefereedReltn, $varRefereedDesct, $varRefereedTitle, $varRefereedVersn)"/>
                    <xsl:choose>
                         <xsl:when test="count($varRefereed[. = '0001']) > 0">
                              <oaf:refereed>
                                   <xsl:value-of select="'0001'"/>
                              </oaf:refereed>
                         </xsl:when>
                         <xsl:when test="count($varRefereed[. = '0002']) > 0">
                              <oaf:refereed>
                                   <xsl:value-of select="'0002'"/>
                              </oaf:refereed>
                         </xsl:when>
                    </xsl:choose>
                    
                    <oaf:dateAccepted>
                         <xsl:value-of select="TransformationFunction:convertString($tf, normalize-space(//datacite:date[@dateType = 'Issued']), 'DateISO8601')"/>
                    </oaf:dateAccepted>
                    
                    <!-- 
                         <oaf:accessrights>
                         <xsl:variable name='varAccessRights' select="TransformationFunction:convertString($tf, (//*[local-name() = 'rights']/(@uri, @rightsURI))[1], 'AccessRights')" />
                         </oaf:accessrights>
                         <xsl:choose>
                         <xsl:when test="not($varAccessRights = 'EMBARGO' and not((xs:date( max( ($varEmbargoEndDate, '0001-01-01') ) ) gt current-date())))">
                         <xsl:value-of select="TransformationFunction:convertString($tf, (//*[local-name() = 'rights']/(@uri, @rightsURI))[1], 'AccessRights')"/>
                         </xsl:when>
                         <xsl:when test="$varAccessRights = 'EMBARGO' and not((xs:date( max( ($varEmbargoEndDate, '0001-01-01') ) ) gt current-date()))">
                         <xsl:value-of select="'OPEN'"/>
                         </xsl:when>
                         </xsl:choose>
                    -->
                    
                    <xsl:variable name='varAccessRights' select="string-join((for $i in ((//*[local-name() = 'rights'], //*[lower-case(local-name())='licensecondition'])/(@*, .)) return  TransformationFunction:convertString($tf, $i, 'AccessRights')), ' ')" />
                    <oaf:accessrights>
                         <xsl:choose>
                              <xsl:when test="contains($varAccessRights, 'OPEN SOURCE')">
                                   <xsl:value-of select="'OPEN SOURCE'"/>
                              </xsl:when>
                              <xsl:when test="contains($varAccessRights, 'OPEN')">
                                   <xsl:value-of select="'OPEN'"/>
                              </xsl:when>
                              <xsl:when test="contains($varAccessRights, 'EMBARGO') and not((xs:date( max( ($varEmbargoEndDate, '0001-01-01') ) ) gt current-date()))">
                                   <xsl:value-of select="'OPEN'"/>
                              </xsl:when>
                              <xsl:when test="contains($varAccessRights, 'EMBARGO') and (xs:date( max( ($varEmbargoEndDate, '0001-01-01') ) ) gt current-date())">
                                   <xsl:value-of select="'EMBARGO'"/>
                              </xsl:when>
                              <xsl:when test="contains($varAccessRights, 'RESTRICTED')">
                                   <xsl:value-of select="'RESTRICTED'"/>
                              </xsl:when>
                              <xsl:when test="contains($varAccessRights, 'CLOSED')">
                                   <xsl:value-of select="'CLOSED'"/>
                              </xsl:when>
                         </xsl:choose>
                    </oaf:accessrights>
                    
                    <xsl:for-each select="//*[local-name()='licenseCondition']
                                          [string-length(.) > 0 or string-length(@uri) > 0]
                                          [(starts-with(@uri, 'http') and (contains(@uri, '://creativecommons.org/licenses/') or contains(@uri, '://creativecommons.org/publicdomain/') or contains(@uri, '://opensource.org/licenses/') or contains(@uri, '://opendatacommons.org/licenses/') or contains(@uri, '://rightsstatements.org/page/') or contains(@uri, '://rightsstatements.org/vocab/') or contains(@uri, '://www.opendefinition.org/licenses/') or contains(@uri, '://www.gnu.org/licenses/') or contains(@uri, '://artlibre.org/licence/') or contains(@uri, '://repositorio.uca.edu.sv/jspui/licencias/') or contains(@uri, '://bibliotecavirtual.unl.edu.ar/licencia/'))) or matches(., '^CC[- ]BY([- ](NC([- ](ND|SA))?|ND|SA))([- ]\d(\.\d)?)?$', 'i')]">
                         <oaf:license>
                              <xsl:value-of select=".[not(./@uri)], .[./@uri]/@uri"/>
                         </oaf:license>
                    </xsl:for-each>
                    
                    <oaf:language>
                         <xsl:value-of select="TransformationFunction:convert($tf, //*[local-name()='language'], 'Languages')" />
                    </oaf:language>
                    
                    <xsl:call-template name="funding" />
                    
                    <oaf:hostedBy>
                         <xsl:attribute name="name">
                              <xsl:value-of select="$varOfficialName"/>
                         </xsl:attribute>
                         <xsl:attribute name="id">
                              <xsl:value-of select="$varDataSourceId"/>
                         </xsl:attribute>
                    </oaf:hostedBy>
                    <oaf:collectedFrom>
                         <xsl:attribute name="name">
                              <xsl:value-of select="$varOfficialName"/>
                         </xsl:attribute>
                         <xsl:attribute name="id">
                              <xsl:value-of select="$varDataSourceId"/>
                         </xsl:attribute>
                    </oaf:collectedFrom>
                    
                    <!-- oaf:container -->
                    <!-- ToDo: set @typ -->
                    <xsl:variable name="varCitation" select="//*[starts-with(local-name(), 'citation')]" />
                    <xsl:variable name="varSource" select="//dc:source[//oaf:datasourceprefix[.=('od______1514', 'od______3158')]]" />
                    <!-- test
                         <xsl:if test="contains($varContainerPid, ':::')">
                         <oaf:container>
                         <xsl:for-each select="tokenize(normalize-space($varContainerPid), ' ')">
                         <xsl:attribute name="{lower-case(substring-before(., ':::'))}" select="substring-before(substring-after(., ':::'), ':::')"/>
                         <xsl:attribute name="typ" select="substring-after(substring-after(., ':::'), ':::')"/>
                         </xsl:for-each>
                         <xsl:choose>
                         <xsl:when test="lower-case(substring-before(., ':::')) = ('issn', 'pissn', 'eissn', 'lissn')">
                         <xsl:attribute name="vol" select="$varCitation[local-name(.) = 'citationVolume']"/>
                         <xsl:attribute name="iss" select="$varCitation[local-name(.) = 'citationIssue']"/>
                         <xsl:attribute name="sp" select="$varCitation[local-name(.) = 'citationStartPage']"/>
                         <xsl:attribute name="ep" select="$varCitation[local-name(.) = 'citationEndPage']"/>
                         </xsl:when>
                         <xsl:when test="lower-case(substring-before(., ':::')) = 'isbn'">
                         <xsl:attribute name="edt" select="$varCitation[local-name(.) = 'citationEdition']"/>
                         <xsl:attribute name="sp" select="$varCitation[local-name(.) = 'citationStartPage']"/>
                         <xsl:attribute name="ep" select="$varCitation[local-name(.) = 'citationEndPage']"/>
                         </xsl:when>
                         </xsl:choose>
                         <xsl:value-of select="$varCitation[local-name(.) = 'citationTitle'], $varSource" />
                         </oaf:container>
                         </xsl:if>
                         test -->
                    
                    <xsl:if test="$varCobjCategory = '0001' and string-length($varContainerPid) > 0">
                         <oaf:journal>
                              <xsl:attribute name='issn' select="substring-before(substring-after($varContainerPidIssn, ' ISSN:::'), ':::')" />
                              <xsl:attribute name='eissn' select="substring-before(substring-after($varContainerPidIssn, ' EISSN:::'), ':::')" />
                              <xsl:attribute name='vol' select="//*[local-name() = 'citationVolume']"/>
                              <xsl:attribute name='iss' select="//*[local-name() = 'citationIssue']"/>
                              <xsl:attribute name='sp' select="//*[local-name() = 'citationStartPage']"/>
                              <xsl:attribute name='ep' select="//*[local-name() = 'citationEndPage']"/>
                              <xsl:value-of select="//*[local-name() = 'citationTitle']" />
                         </oaf:journal>
                    </xsl:if>
                    
                    <!-- Huelva marks L, E ISSNs as ISSNs, with mark within field in spaces or after blanc -->
                    <!-- Qeios declares many records as text, although many seem to be definitions which are also related to 'journal' volumes/issues -->
                    
                    <!-- oaf:fulltext-->
                    <!-- toDo: clarify if fulltext should be filled when URL given and rights oa -->
                    <!--
                         <xsl:if test="//*[local-name() = 'file']">
                         <oaf:fulltext>
                         <xsl:value-of select="//*[local-name() = 'file']"/>
                         </oaf:fulltext>
                         </xsl:if>
                    -->
                    
               </metadata>
               <xsl:copy-of select="//*[local-name() = 'about']" />
          </record>
     </xsl:template>
     
     <xsl:template match="node()|@*">
          <xsl:copy  copy-namespaces="no">
               <xsl:apply-templates select="node()|@*"/>
          </xsl:copy>
     </xsl:template>
     
     <xsl:template match="//*[local-name() = 'metadata']//*[local-name() = 'resource']">
          <xsl:copy copy-namespaces="no">
               <xsl:apply-templates select="node()|@*"/>
          </xsl:copy>
     </xsl:template>
     
     <xsl:template name="funding">
          <!-- funding -->
          <xsl:for-each select="//*[local-name()='fundingReference'][./*[local-name()='awardNumber']]">
               <xsl:choose>
                    <!-- FP7 -->
                    <xsl:when test="(./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100004963|10.13039/100011199|10.13039/100011102)\s*$')] 
                                    or (./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100000780)$')] and ./*[local-name()='fundingStream'] = ('Framework Programme Seven', 'FP7'))
                                    or (./*[local-name()='funderName'] = 'European Commission' and ./*[local-name()='fundingStream'] = ('Framework Programme Seven', 'FP7')))
                                    and ./*[local-name()='awardNumber'][matches(., '.*(^|[^\d])\d\d\d\d\d\d($|[^\d]).*')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varFP7, replace(./*[local-name()='awardNumber'], '.*(^|[^\d])(\d\d\d\d\d\d)($|[^\d]).*', '$2'))"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- H2020 (Horizon 2020 Framework Programme) -->
                    <!-- -->
                    <xsl:when test="(./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/100010661)\s*')]
                                    or (./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100000780)$')] and ./*[local-name()='fundingStream'] = ('Horizon 2020 Framework Programme', 'H2020'))
                                    or (./*[local-name()='funderName'] = 'European Commission' and ./*[local-name()='fundingStream'] = ('Horizon 2020 Framework Programme', 'H2020')))
                                    and ./*[local-name()='awardNumber'][matches(., '.*(^|[^\d])\d\d\d\d\d\d($|[^\d]).*')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varH2020, replace(./*[local-name()='awardNumber'], '.*(^|[^\d])(\d\d\d\d\d\d)($|[^\d]).*', '$2'))"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- AKA -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100002341)\s*$') ]
                                    or ./*[local-name()='funderName'][contains(., 'Suomen Akatemia') or contains(., 'Academy of Finland')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varAKA, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- ARC -->
                    <xsl:when test="(./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100000923)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Australian Research Council')]) 
                                    and ./*[local-name()='awardNumber'][matches(., '^\d{6}$')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varAKA, replace(./*[local-name()='awardNumber'], '.*(^\d{6}$).*', '$2'))"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- CONICYT -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100002848)\s*$')]
                                    or ./*[local-name()='funderName'][contains(., 'Comisión Nacional de Investigación Científica y Tecnológica') or contains(., 'CONICYT')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varCONICYT, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- DFG -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100001659)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Deutsche Forschungsgemeinschaft') or contains(., 'DFG') or contains(., 'Deutsche Forschungsgemeinschaft (DFG)')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varDFG, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!---->
                    <!-- FCT -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100001871)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Fundação para a Ciência e a Tecnologia')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varFCT, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- FWF -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100002428)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Fonds zur Förderung der Wissenschaftlichen Forschung') or contains(., 'Austrian Science Fund')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varFCT, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- MESTD -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100001871)\s*$')] 
                                    or ./*[local-name()='funderName'][(contains(., 'Ministarstvo Prosvete, Nauke i Tehnolo') and contains(., 'kog Razvoja')) or contains(., 'MESTD')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varMESTD, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- MZOS -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100006588)\s*$')]
                                    or ./*[local-name()='funderName'][contains(., 'Ministarstvo Znanosti, Obrazovanja i Sporta') or contains(., 'Ministry of Science, Education and Sports')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varMZOS, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- NHMRC -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100000925)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'National Health and Medical Research Council') or contains(., 'NHMRC')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varNHMRC, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- NIH -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/100000002)\s*$')]
                                    or ./*[local-name()='funderName'][contains(., 'National Institutes of Health')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varNIH, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- NSF -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/100000001)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'National Science Foundation')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varNSF, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- NWO -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100003246)\s*$')]
                                    or ./*[local-name()='funderName'][contains(., 'Netherlands Organisation for Scientific Research') or contains(., 'Nederlandse Organisatie voor Wetenschappelijk Onderzoek')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varNWO, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- RCUK -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100000690)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Research Councils UK') or contains(., 'RCUK')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varRCUK, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- SFI -->
                    <xsl:when test="(./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100001602)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Science Foundation Ireland')]) 
                                    and ./*[local-name()='awardNumber'][matches(., '.*([\dA-Za-z\.\-]+/)+[\dA-Za-z\.\-]+.*')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varSFI, replace(./*[local-name()='awardNumber'], '.*(^|\s)(([\dA-Za-z\.\-]+/)+[\dA-Za-z\.\-]+)($|\s).*', '$2'))"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- SNSF -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100001711)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Swiss National Science Foundation') or contains(., 'Schweizerischer Nationalfonds zur Förderung der Wissenschaftlichen Forschung')]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varSNSF, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- TUBITAK -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/501100004410)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Turkish National Science and Research Council') or (contains(., 'Türkiye Bilimsel ve Teknolojik Ara') and contains(., 'rma Kurumu'))]">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varTUBITAK, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
                    <!-- WT -->
                    <xsl:when test="./*[local-name()='funderIdentifier'][matches(., '.*(doi.org/)?(10.13039/100004440)\s*$')] 
                                    or ./*[local-name()='funderName'][contains(., 'Wellcome Trust') or . = 'WT']">
                         <oaf:projectid>
                              <xsl:value-of select="concat($varWT, ./*[local-name()='awardNumber'])"/>
                         </oaf:projectid>
                    </xsl:when>
               </xsl:choose> 
          </xsl:for-each>
     </xsl:template>
     
     <!-- toDo: improve by selecting all attributes -->
     <xsl:template match="//*[local-name()='nameIdentifier']">
          <datacite:nameIdentifier>
               <xsl:attribute name="nameIdentifierScheme" select="./@*[local-name()=('nameIdentifierScheme')]"/>
               <xsl:attribute name="schemeURI" select="./@*[local-name()=('schemeURI')]"/>
               <xsl:choose>
                    <xsl:when test=".[contains(., 'orcid.org/')]">
                         <xsl:value-of select="substring-after(., 'orcid.org/')" />     
                    </xsl:when>
                    <xsl:when test=".[contains(., 'researcherid.com/rid/')]">
                         <xsl:value-of select="substring-after(., 'researcherid.com/rid/')" />     
                    </xsl:when>
                    <xsl:otherwise>
                         <xsl:value-of select="." />     
                    </xsl:otherwise>
               </xsl:choose>
          </datacite:nameIdentifier>
     </xsl:template>
     
     <xsl:template match="//*[local-name() = 'header']">
          <header xmlns="http://www.openarchives.org/OAI/2.0/">
               <!--
                    <xsl:copy copy-namespaces="no">
               -->
               <xsl:apply-templates  select="node()|@*"/>
               <xsl:element name="dr:dateOfTransformation">
                    <xsl:value-of select="$transDate"/>
               </xsl:element>
               <!--
                    </xsl:copy>
               -->
          </header>
     </xsl:template>
     
</xsl:stylesheet>