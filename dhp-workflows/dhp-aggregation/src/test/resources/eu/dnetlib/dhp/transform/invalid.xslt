<xsl:stylesheet
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:oaire="http://namespace.openaire.eu/schema/oaire/"
	xmlns:vocabulary="http://eu/dnetlib/transform/clean"
	xmlns:dateCleaner="http://eu/dnetlib/transform/dateISO"
	xmlns:oaf="http://namespace.openaire.eu/oaf"
	xmlns:oai="http://www.openarchives.org/OAI/2.0/"
	xmlns:datacite="http://datacite.org/schema/kernel-4"
	xmlns:dri="http://www.driver-repository.eu/namespace/dri"
	xmlns:xs="http://www.w3.org/2001/XMLSchema"
	xmlns:dr="http://www.driver-repository.eu/namespace/dr"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:dc="http://purl.org/dc/elements/1.1/"
	exclude-result-prefixes="xsl vocabulary dateCleaner" version="2.0">
	<xsl:param name="varOfficialName" />
	<xsl:param name="varDataSourceId" />
	<xsl:param name="varFP7" select="'corda_______::'" />
	<xsl:param name="varH2020" select="'corda__h2020::'" />
	<xsl:param name="repoCode"
		select="substring-before(//*[local-name() = 'header']/*[local-name()='recordIdentifier'], ':')" />
	<xsl:param name="index" select="0" />
	<xsl:param name="transDate" select="current-dateTime()" />
asdf;klamsdof'sdn &
	<xsl:template match="/asdfasdf asdf&">
		c:format'" />
				</xsl:call-template>
				<xsl:call-template name="allElements">
					<xsl:with-param name="sourceElement" select="//dc:type" />
					<xsl:with-param name="targetElement" select="'dc:type'" />
				</xsl:call-template>
				<xsl:call-template name="allElements">
					<xsl:with-param name="sourceElement" select="//dc:source" />
					<xsl:with-param name="targetElement" select="'dc:source'" />
				</xsl:call-template>
				<dc:language>
					<xsl:value-of select="vocabulary:clean( //dc:language, 'dnet:languages')" />
				</dc:language>
				<xsl:call-template name="allElements">
					<xsl:with-param name="sourceElement" select="//dc:rights" />
					<xsl:with-param name="targetElement" select="'dc:rights'" />
				</xsl:call-template>
				<xsl:call-template name="allElements">
					<xsl:with-param name="sourceElement" select="//dc:relation[not(starts-with(.,'info:cnr-pdr'))]" />
					<xsl:with-param name="targetElement" select="'dc:relation'" />
				</xsl:call-template>
				
				<xsl:call-template name="allElements">
					<xsl:with-param name="sourceElement" select="//dc:identifier[starts-with(., 'http')]" />
					<xsl:with-param name="targetElement" select="'dc:identifier'" />
				</xsl:call-template>
				<xsl:for-each select="//dc:relation">
					<xsl:if	test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', 'i')">
						<oaf:projectid>
							<xsl:value-of select="concat($varFP7, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/fp7/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))" />
						</oaf:projectid>
					</xsl:if>
					<xsl:if test="matches(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', 'i')">
						<oaf:projectid>
							<xsl:value-of select="concat($varH2020, replace(normalize-space(.), '(info:eu-repo/grantagreement/ec/h2020/)(\d\d\d\d\d\d)(.*)', '$2', 'i'))" />
						</oaf:projectid>
					</xsl:if>
				</xsl:for-each>
				
				<oaf:accessrights>
					<xsl:value-of select="vocabulary:clean( //dc:rights, 'dnet:access_modes')" />
				</oaf:accessrights>
				
				<xsl:variable name="varCobjCategory" select="vocabulary:clean( //dc:type, 'dnet:publication_resource')" />
				<xsl:variable name="varSuperType" select="vocabulary:clean( $varCobjCategory, 'dnet:result_typologies')" />				
				<dr:CobjCategory type="{$varSuperType}"><xsl:value-of select="$varCobjCategory" /></dr:CobjCategory>
				
				
				<xsl:variable name="varRefereedConvt" select="for $i in (//dc:type, //dc:description, //oai:setSpec) return vocabulary:clean( normalize-space($i), 'dnet:review_levels')" />
				<xsl:variable name="varRefereedIdntf" select="(//*[string(node-name(.)) = 'dc:identifier' and matches(lower-case(.), '(^|.*[\.\-_/\s\(\)%\d#])pre[\.\-_/\s\(\)%\d#]?prints?([\.\-_/\s\(\)%\d#].*)?$')][count(//dc:identifier) = 1]/'0002', //*[string(node-name(.)) = 'dc:identifier' and matches(lower-case(.), '(^|.*[\.\-_/\s\(\)%\d#])refereed([\.\-_/\s\(\)\d%\d#].*)?$')]/'0001', //*[string(node-name(.)) = 'dc:identifier' and matches(lower-case(.), '.*-peer-reviewed-(fulltext-)?article-.*')]/'0001')" />
				<xsl:variable name="varRefereedSourc" select="//*[string(node-name(.)) = ('dc:source', 'dc:publisher') and matches(lower-case(.), '^(.*\s)?pre[\s\-_]*prints?([\s\.,].*)?$')]/'0002'" />
				<xsl:variable name="varRefereedDescr" select="(//dc:description[matches(lower-case(.), '.*(this\s*book|this\s*volume|it)\s*constitutes\s*the\s*(thoroughly\s*)?refereed') or matches(lower-case(.), '.*peer[\.\-_/\s\(\)]?review\s*under\s*responsibility\s*of.*') or matches(lower-case(.), '(this|a)\s*(article|preprint)\s*(has\s*been\s*)?(peer[\-\s]*)?reviewed\s*and\s*recommended\s*by\s*peer[\-\s]*community')]/'0001', //dc:description[matches(., '^version\s*(préliminaire.*|preliminary.*|0$)')]/'0002')" />
				<xsl:variable name="varRefereedTitle" select="(//dc:title[matches(lower-case(.), '.*\[.*peer[\s\-\._]*review\s*:.*\]\s*$')]/'0001', //dc:title[matches(lower-case(.), '.*\(\s*pre[\s\-\._]*prints?\s*\)\s*$')]/'0002')" />
				<xsl:variable name="varRefereedSubjt" select="(//dc:subject[matches(lower-case(.), '^\s*refereed\s*$')][//oaf:datasourceprefix = 'narcis______']/'0001', //dc:subject[matches(lower-case(.), '^\s*no[nt].{0,3}refereed\s*$')][//oaf:datasourceprefix = 'narcis______']/'0002')" />
				<xsl:variable name="varRefereed" select="($varRefereedConvt, $varRefereedIdntf, $varRefereedSourc, $varRefereedDescr, $varRefereedTitle, $varRefereedSubjt)" />
				<xsl:choose>
					<xsl:when test="count($varRefereed[. = '0001']) &gt; 0">
						<oaf:refereed>
							<xsl:value-of select="'0001'" />
						</oaf:refereed>
					</xsl:when>
					<xsl:when test="count($varRefereed[. = '0002']) &gt; 0">
						<oaf:refereed>
							<xsl:value-of select="'0002'" />
						</oaf:refereed>
					</xsl:when>
				</xsl:choose>
				
				<oaf:dateAccepted>
					<xsl:value-of select="dateCleaner:dateISO( //dc:date[1] )" />
				</oaf:dateAccepted>
				
				<xsl:if test="//dc:relation[starts-with(., 'http')] and //dc:rights[.='info:eu-repo/semantics/openAccess']">
					<oaf:fulltext>
						<xsl:value-of select="//dc:relation[starts-with(., 'http')]" />
					</oaf:fulltext>
				</xsl:if>
				
				<oaf:hostedBy name="{$varOfficialName}" id="{$varDataSourceId}" />
				<oaf:collectedFrom name="{$varOfficialName}" id="{$varDataSourceId}" />

				<xsl:variable name="varKnownFileEndings" select="('.bmp', '.doc', '.docx', '.epub', '.flv', '.jpeg', '.jpg', '.m4v', '.mp4', '.mpg', '.odp', '.pdf', '.png', '.ppt', '.tiv', '.txt', '.xls', '.xlsx', '.zip')" />
				<xsl:variable name="varIdDoi" select="distinct-values((//dc:identifier[starts-with(., '10.')][matches(., '(10[.][0-9]{4,}[^\s/&gt;]*/[^\s&gt;]+)')], //dc:identifier[starts-with(., 'http') and (contains(., '://dx.doi.org/10.') or contains(., '://doi.org/10.'))]/substring-after(., 'doi.org/'), //dc:identifier[starts-with(lower-case(.), 'doi:10.')]/substring-after(lower-case(.), 'doi:')))" />
				<xsl:for-each select="$varIdDoi">
					<oaf:identifier identifierType="doi">
						<xsl:value-of select="." />
					</oaf:identifier>
				</xsl:for-each>
				
				<xsl:variable name="varIdHdl" select="distinct-values(//dc:identifier[starts-with(., 'http') and contains(., '://hdl.handle.net/')]/substring-after(., 'hdl.handle.net/'))" />
				<xsl:for-each select="$varIdHdl" >
					<oaf:identifier identifierType="handle">
						<xsl:value-of select="." />
					</oaf:identifier>
				</xsl:for-each>
				
				<xsl:variable name="varIdUrn" select="distinct-values(//dc:identifier[starts-with(., 'urn:nbn:nl:') or starts-with(., 'URN:NBN:NL:')])" />
				<xsl:for-each select="$varIdUrn">
					<oaf:identifier identifierType="urn">
						<xsl:value-of select="." />
					</oaf:identifier>
				</xsl:for-each>
				
				<xsl:variable name="varOrigBaseUrl" select="//*[local-name() = 'about']/*[local-name() = 'provenance']//*[local-name() = 'originDescription' and not(./*[local-name() = 'originDescription'])]/*[local-name() = 'baseURL']" />
				<xsl:variable name="varIdLdpg" select="distinct-values(//dc:identifier[(contains(substring-after(., '://'), '/') and contains($varOrigBaseUrl, substring-before(substring-after(., '://'), '/'))) or (contains(substring-after(., '://'), ':') and contains($varOrigBaseUrl, substring-before(substring-after(., '://'), ':')))][not(replace(lower-case(.), '.*(\.[a-z]*)$', '$1') = $varKnownFileEndings)])" />
				<xsl:for-each select="$varIdLdpg">
					<oaf:identifier identifierType="landingPage">
						<xsl:value-of select="." />
					</oaf:identifier>
				</xsl:for-each>
				
				<xsl:variable name="varIdUrl" select="distinct-values(//dc:identifier[starts-with(., 'http')][not(contains(., '://dx.doi.org/') or contains(., '://doi.org/') or contains(., '://hdl.handle.net/'))][count(index-of($varIdLdpg, .)) = 0])" />
				<xsl:for-each select="$varIdUrl">
					<oaf:identifier identifierType="url">
						<xsl:value-of select="." />
					</oaf:identifier>
				</xsl:for-each>
				
				<xsl:for-each select="//oai:setSpec">
					<xsl:variable name="rorDsId" select="vocabulary:clean(., 'cnr:institutes')" />
					<xsl:if test="contains($rorDsId, '/ror.org/')">
						<oaf:relation relType="resultOrganization" subRelType="affiliation" relClass="hasAuthorInstitution">
							<xsl:value-of select="concat('ror_________::', $rorDsId)" />
						</oaf:relation>
					</xsl:if>
				</xsl:for-each>
				
			</metadata>
			
			<xsl:copy-of select="//*[local-name() = 'about']" />
		</record>
	</xsl:template>
	
	
	<xsl:template name="allElements">
		<xsl:param name="sourceElement" />
		<xsl:param name="targetElement" />
	
		<xsl:for-each select="$sourceElement">
			<xsl:element name="{$targetElement}">
				<xsl:value-of select="normalize-space(.)" />
			</xsl:element>
		</xsl:for-each>
	</xsl:template>
	
	<xsl:template match="//*[local-name() = 'header']">
		<xsl:copy>
			<xsl:apply-templates select="node()|@*" />
			<xsl:element name="dr:dateOfTransformation">
				<xsl:value-of select="$transDate" />
			</xsl:element>
		</xsl:copy>
	</xsl:template>
	
	<xsl:template match="node()|@*">
		<xsl:copy>
			<xsl:apply-templates select="node()|@*" />
		</xsl:copy>
	</xsl:template>
	
</xsl:stylesheet>