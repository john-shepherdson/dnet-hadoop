<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:dnet="eu.dnetlib.data.transform.xml.AbstractDNetXsltFunctions" version="2.0">

    <xsl:param    name="varDataSourceId" select="string('openaire____::eurocris')"/>
    <xsl:variable name="namespacePrefix" select="string('eurocrisdris')"/>

    <xsl:template match="/">
        <record>

            <xsl:copy-of select=".//*[local-name() = 'header']"/>
            <metadata>
                <xsl:variable name="rid" select="replace(normalize-space(//itemListElement/oat_id),'https://api.eurocris.org/dris/entries/dris','')"/>
                <xsl:variable name="datasourceId" select="concat($namespacePrefix, '::', $rid)"/>
                <xsl:variable name="apiId" select="concat('api_________::', $datasourceId, '::0')"/>
                <xsl:variable name="repositoryType">
                    <xsl:choose>
                        <xsl:when test="lower-case(normalize-space(//scope)) = 'vocabs:scopes/classcerif00889'">
                            <xsl:value-of select="string('crissystem::institutional')"/>
                        </xsl:when>
                        <xsl:when test="lower-case(normalize-space(//scope)) = 'vocabs:scopes/classcerif00939'">
                            <xsl:value-of select="string('crissystem::national')"/>
                        </xsl:when>
                        <xsl:when test="lower-case(normalize-space(//scope)) = 'vocabs:scopes/classcerif00122'">
                            <xsl:value-of select="string('crissystem::funder')"/>
                        </xsl:when>
                        <xsl:when test="lower-case(normalize-space(//scope)) = 'vocabs:scopes/classcerif00938'">
                            <xsl:value-of select="string('crissystem::regional')"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="string('crissystem')"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:variable>

                <xsl:variable name="oCountry" select="upper-case(normalize-space(//iso_3166_codes/Alpha2))"/>

                <xsl:variable name="repositoryPlatform">
                    <xsl:for-each select="//itemListElement/oat_included">
                        <xsl:choose>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01750'">
                                <xsl:value-of select="string('COBISS.CG')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01751'">
                                <xsl:value-of select="string('Converis')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01752'">
                                <xsl:value-of select="string('CRIStin')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01753'">
                                <xsl:value-of select="string('CSpace')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01754'">
                                <xsl:value-of select="string('DSpace-CRIS')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01755'">
                                <xsl:value-of select="string('EPrints')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01756'">
                                <xsl:value-of select="string('Haplo')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01757'">
                                <xsl:value-of select="string('Ideate')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01758'">
                                <xsl:value-of select="string('In-House-Built')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01759'">
                                <xsl:value-of select="string('IRINS')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01760'">
                                <xsl:value-of select="string('IRIS')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01761'">
                                <xsl:value-of select="string('Metis')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01762'">
                                <xsl:value-of select="string('Omega-PSIR')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01763'">
                                <xsl:value-of select="string('PeopleSoft')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01764'">
                                <xsl:value-of select="string('Pure')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01765'">
                                <xsl:value-of select="string('SIGMA Research')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01766'">
                                <xsl:value-of select="string('SoleCRIS')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01767'">
                                <xsl:value-of select="string('Symplectic Elements')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01768'">
                                <xsl:value-of select="string('UXXI-INV')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01769'">
                                <xsl:value-of select="string('VIVO')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01770'">
                                <xsl:value-of select="string('Worktribe')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01854'">
                                <xsl:value-of select="string('HISinOne-RES')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01960'">
                                <xsl:value-of select="string('Dialnet CRIS')"/>
                            </xsl:when>
                            <xsl:when test="lower-case(substring(normalize-space(oat_id),string-length(oat_id) -30 +1,30)) = 'cris-platforms/classcerif01961'">
                                <xsl:value-of select="string('Esploro')"/>
                            </xsl:when>
                        </xsl:choose>
                    </xsl:for-each>
                </xsl:variable>

                <xsl:variable name="repositoryPlatform" select="upper-case(normalize-space(//itemListElement/oat_included[3]/label/en))"/>

                <xsl:variable name="contenttypes">
                    <xsl:for-each select=".//content_types">
                        <xsl:if test="position() > 1">-</xsl:if>
                        <xsl:value-of select="."/>
                    </xsl:for-each>
                </xsl:variable>
                <xsl:variable name="languages">
                    <xsl:for-each select=".//content_languages">
                        <xsl:if test="position() > 1">,</xsl:if>
                        <xsl:value-of select="."/>
                    </xsl:for-each>
                </xsl:variable>

                <xsl:variable name="subjects">
                    <xsl:for-each select=".//content_subjects/label">
                        <xsl:if test="position() > 1">@@</xsl:if>
                        <xsl:value-of select="."/>
                    </xsl:for-each>
                </xsl:variable>

                <ROWS>
                    <ROW table="dsm_datasources">
                        <FIELD name="_dnet_resource_identifier_">
                            <xsl:value-of select="$datasourceId"/>
                        </FIELD>
                        <FIELD name="id">
                            <xsl:value-of select="$datasourceId"/>
                        </FIELD>
                        <FIELD name="officialname">
                            <xsl:choose>
                                <xsl:when test="//itemListElement/name">
                                    <xsl:value-of select="normalize-space(//itemListElement/name[1])"/>
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:value-of select="normalize-space(//repository_metadata/name[1]/acronym)" />
                                </xsl:otherwise>
                            </xsl:choose>
                        </FIELD>
                        <FIELD name="englishname">
                            <xsl:choose>
                                <xsl:when test="//itemListElement/acronym">
                                    <xsl:value-of select="normalize-space(//itemListElement/acronym)" />
                                </xsl:when>
                                <xsl:otherwise>
                                    <xsl:value-of select="normalize-space(//repository_metadata/name[1]/name[1])"/>
                                </xsl:otherwise>
                            </xsl:choose>
                        </FIELD>
                        <FIELD name="websiteurl">
                            <xsl:value-of select="normalize-space(//itemListElement/uri)"/>
                        </FIELD>
                        <FIELD name="contactemail">
                            <xsl:value-of select="normalize-space(//pEmail)"/>
                        </FIELD>
                        <xsl:if test="not(normalize-space(//organisation/location/latitude) = 'null')">
                            <FIELD name="latitude" type="float">
                                <xsl:value-of select="normalize-space(//organisation/location/latitude)"/>
                            </FIELD>
                        </xsl:if>
                        <xsl:if test="normalize-space(//organisation/location/latitude) = 'null'">
                            <FIELD name="latitude" type="float"/>
                        </xsl:if>
                        <xsl:if test="not(normalize-space(//organisation/location/longitude) = 'null')">
                            <FIELD name="longitude" type="float">
                                <xsl:value-of select="normalize-space(//organisation/location/longitude)"/>
                            </FIELD>
                        </xsl:if>
                        <xsl:if test="normalize-space(//organisation/location/longitude) = 'null'">
                            <FIELD name="longitude" type="float"/>
                        </xsl:if>
                        <FIELD name="namespaceprefix">
                            <xsl:value-of select="dnet:generateNsPrefix('dris', replace($rid,'dris',''))"/>
                        </FIELD>
                        <FIELD name="languages">
                            <xsl:value-of select="normalize-space($languages)"/>
                        </FIELD>
                        <FIELD name="od_contenttypes">
                            <xsl:value-of select="normalize-space($contenttypes)"/>
                        </FIELD>
                        <FIELD name="collectedfrom">
                            <xsl:value-of select="$varDataSourceId"/>
                        </FIELD>
                        <FIELD name="typology">
                            <xsl:value-of select="$repositoryType"/>
                        </FIELD>
                        <FIELD name="provenanceaction">sysimport:crosswalk:entityregistry</FIELD>
                        <FIELD name="platform">
                            <xsl:value-of select="$repositoryPlatform"/>
                        </FIELD>
                        <FIELD name="description">
                            <xsl:value-of select="normalize-space(//itemListElement/description)"/>
                        </FIELD>

                        <FIELD name="subjects">
                            <xsl:value-of select="normalize-space($subjects)"/>
                        </FIELD>
                        <xsl:if test="//policies/preservation_policy/version_control/policy[./text() = 'updated_versions_allowed']">
                            <FIELD name="versioning" type="boolean">true</FIELD>
                        </xsl:if>
                    </ROW>

                    <ROW table="dsm_api">
                        <FIELD name="_dnet_resource_identifier_">
                            <xsl:value-of select="$apiId"/>
                        </FIELD>
                        <FIELD name="id">
                            <xsl:value-of select="$apiId"/>
                        </FIELD>
                        <FIELD name="protocol">oai</FIELD>
                        <FIELD name="datasource">
                            <xsl:value-of select="$datasourceId"/>
                        </FIELD>
                        <FIELD name="contentdescription">metadata</FIELD>
                        <FIELD name="typology">
                            <xsl:value-of select="$repositoryType"/>
                        </FIELD>
                        <FIELD name="baseurl">
                            <xsl:value-of select="normalize-space(//itemListElement/openaireCrisEndpointURL)"/>
                        </FIELD>
                        <FIELD name="metadata_identifier_path">//*[local-name()='header']/*[local-name()='identifier']</FIELD>
                    </ROW>

                    <ROW table="dsm_apiparams">
                        <FIELD name="_dnet_resource_identifier_">
                            <xsl:value-of select="concat($apiId, '@@format')"/>
                        </FIELD>
                        <FIELD name="api">
                            <xsl:value-of select="$apiId"/>
                        </FIELD>
                        <FIELD name="param">format</FIELD>
                        <FIELD name="value">oai_cerif_openaire</FIELD>
                    </ROW>

                    <!--                                        <xsl:for-each select="//organisation">    -->

                    <xsl:variable name="oUrl" select="normalize-space(//itemListElement/oat_included[4]/oat_id)"/>
                    <xsl:variable name="preferredNameLanguage" select="./name/preferred_phrases[./value = 'name'][1]/language[1]"/>
                    <xsl:variable name="oName">
                        <xsl:choose>
                            <xsl:when test="string-length($preferredNameLanguage[1]/text()) > 0 and ./name[./language = $preferredNameLanguage]/name">
                                <xsl:value-of select="./name[./language = $preferredNameLanguage]/name/text()" />
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="//name"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>
                    <xsl:variable name="oAcronym">
                        <xsl:choose>
                            <xsl:when test="string-length($preferredNameLanguage[1]/text()) > 0 and ./name[./language = $preferredNameLanguage]/acronym">
                                <xsl:value-of select="./name[./language = $preferredNameLanguage]/acronym" />
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="//acronym"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </xsl:variable>

                    <xsl:variable name="organizationId" select="translate(translate(concat($namespacePrefix, '::', $oName, '_', $oCountry), ' ', '_'), '&amp;quot;', '')"/>
                    <xsl:if test="string-length($oName) > 0">
                        <ROW table="dsm_organizations">
                            <FIELD name="_dnet_resource_identifier_">
                                <xsl:value-of select="$organizationId"/>
                            </FIELD>
                            <FIELD name="id">
                                <xsl:value-of select="$organizationId"/>
                            </FIELD>
                            <FIELD name="legalname">
                                <xsl:value-of select="$oName"/>
                            </FIELD>
                            <FIELD name="legalshortname">
                                <xsl:value-of select="normalize-space($oAcronym)"/>
                            </FIELD>
                            <FIELD name="websiteurl">
                                <xsl:choose>
                                    <xsl:when test="starts-with(normalize-space($oUrl), 'http')">
                                        <xsl:value-of select="normalize-space($oUrl)"/>
                                    </xsl:when>
                                    <xsl:when test="string-length(normalize-space($oUrl)) > 0">
                                        <xsl:value-of select="concat('http://', normalize-space($oUrl))"/>
                                    </xsl:when>
                                </xsl:choose>
                            </FIELD>

                            <FIELD name="country">
                                <xsl:choose>
                                    <xsl:when test="$oCountry = 'UK'">GB</xsl:when>
                                    <xsl:otherwise>
                                        <xsl:value-of select="$oCountry"/>
                                    </xsl:otherwise>
                                </xsl:choose>
                            </FIELD>

                            <FIELD name="collectedfrom">
                                <xsl:value-of select="$varDataSourceId"/>
                            </FIELD>
                            <FIELD name="provenanceaction">sysimport:crosswalk:entityregistry</FIELD>
                        </ROW>

                        <ROW table="dsm_datasource_organization">
                            <FIELD name="_dnet_resource_identifier_">
                                <xsl:value-of select="concat($datasourceId, '@@', $organizationId)"/>
                            </FIELD>
                            <FIELD name="datasource">
                                <xsl:value-of select="$datasourceId"/>
                            </FIELD>
                            <FIELD name="organization">
                                <xsl:value-of select="$organizationId"/>
                            </FIELD>
                        </ROW>
                    </xsl:if>

                    <!--                                        </xsl:for-each> -->

                    <!-- EOSC datasource support , TODO -->
                    <xsl:choose>
                    <xsl:when test="$repositoryType = 'crissystem::institutional'">
                     <ROW table="dsm_datasources_eosc">
                        <FIELD name="id"><xsl:value-of select="$datasourceId"/></FIELD>
                        <FIELD name="_dnet_resource_identifier_">
                            <xsl:value-of select="$datasourceId"/>
                        </FIELD>
                        <FIELD name="jurisdiction"><xsl:value-of select="string('Institutional')"/></FIELD>
                    </ROW>
                    </xsl:when>
                    <xsl:when test="$repositoryType = 'crissystem::thematic'">
                     <ROW table="dsm_datasources_eosc">
                        <FIELD name="id"><xsl:value-of select="$datasourceId"/></FIELD>
                        <FIELD name="_dnet_resource_identifier_">
                            <xsl:value-of select="$datasourceId"/>
                        </FIELD>
                        <FIELD name="thematic" type="boolean">true</FIELD>
                    </ROW>
                    </xsl:when>

                    </xsl:choose>

                </ROWS>
            </metadata>
        </record>
    </xsl:template>

</xsl:stylesheet>