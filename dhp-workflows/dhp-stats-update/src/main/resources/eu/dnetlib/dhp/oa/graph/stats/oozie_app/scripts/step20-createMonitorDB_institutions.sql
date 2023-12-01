drop database if exists TARGET cascade;
create database if not exists TARGET;

create table TARGET.result stored as parquet as
    select distinct * from (
        select * from SOURCE.result r where exists (select 1 from SOURCE.result_organization ro where ro.id=r.id and ro.organization in (
             'openorgs____::b84450f9864182c67b8611b5593f4250', --"Athena Research and Innovation Center In Information Communication & Knowledge Technologies', --ARC"
             'openorgs____::d41cf6bd4ab1b1362a44397e0b95c975', --National Research Council
             'openorgs____::d2a09b9d5eabb10c95f9470e172d05d2', --??? Not exists ??
             'openorgs____::d169c7407dd417152596908d48c11460', --Masaryk University
             'openorgs____::1ec924b1759bb16d0a02f2dad8689b21', --University of Belgrade
             'openorgs____::0ae431b820e4c33db8967fbb2b919150', --University of Helsinki
             'openorgs____::759d59f05d77188faee99b7493b46805', --University of Minho
             'openorgs____::cad284878801b9465fa51a95b1d779db', --Universidad Politécnica de Madrid
             'openorgs____::eadc8da90a546e98c03f896661a2e4d4', --University of Göttingen
             'openorgs____::c0286313e36479eff8676dba9b724b40', --National and Kapodistrian University of Athens
             -- 'openorgs____::c80a8243a5e5c620d7931c88d93bf17a', --Université Paris Diderot
             'openorgs____::c08634f0a6b0081c3dc6e6c93a4314f3', --Bielefeld University
             'openorgs____::6fc85e4a8f7ecaf4b0c738d010e967ea', --University of Southern Denmark
             'openorgs____::3d6122f87f9a97a99d8f6e3d73313720', --Humboldt-Universität zu Berlin
             'openorgs____::16720ada63d0fa8ca41601feae7d1aa5', --TU Darmstadt
             'openorgs____::ccc0a066b56d2cfaf90c2ae369df16f5', --KU Leuven
             'openorgs____::4c6f119632adf789746f0a057ed73e90', --University of the Western Cape
             'openorgs____::ec3665affa01aeafa28b7852c4176dbd', --Rudjer Boskovic Institute
             'openorgs____::5f31346d444a7f06a28c880fb170b0f6', --Ghent University
             'openorgs____::2dbe47117fd5409f9c61620813456632', --University of Luxembourg
             'openorgs____::6445d7758d3a40c4d997953b6632a368', --National Institute of Informatics (NII)
             'openorgs____::b77c01aa15de3675da34277d48de2ec1', -- Valencia Catholic University Saint Vincent Martyr
             'openorgs____::7fe2f66cdc43983c6b24816bfe9cf6a0', -- Unviersity of Warsaw
             'openorgs____::15e7921fc50d9aa1229a82a84429419e', -- University Of Thessaly
             'openorgs____::11f7919dadc8f8a7251af54bba60c956', -- Technical University of Crete
             'openorgs____::84f0c5f5dbb6daf42748485924efde4b', -- University of Piraeus
             'openorgs____::4ac562f0376fce3539504567649cb373', -- University of Patras
             'openorgs____::3e8d1f8c3f6cd7f418b09f1f58b4873b', -- Aristotle University of Thessaloniki
             'openorgs____::3fcef6e1c469c10f2a84b281372c9814', -- World Bank
             'openorgs____::1698a2eb1885ef8adb5a4a969e745ad3', -- École des Ponts ParisTech
             'openorgs____::e15adb13c4dadd49de4d35c39b5da93a', -- Nanyang Technological University
             'openorgs____::4b34103bde246228fcd837f5f1bf4212', -- Autonomous University of Barcelona
             'openorgs____::72ec75fcfc4e0df1a76dc4c49007fceb', -- McMaster University
             'openorgs____::51c7fc556e46381734a25a6fbc3fd398', -- University of Modena and Reggio Emilia
             'openorgs____::235d7f9ad18ecd7e6dc62ea4990cb9db', -- Bilkent University
             'openorgs____::31f2fa9e05b49d4cf40a19c3fed8eb06', -- Saints Cyril and Methodius University of Skopje
             'openorgs____::db7686f30f22cbe73a4fde872ce812a6', -- University of Milan
             'openorgs____::b8b8ca674452579f3f593d9f5e557483',  -- University College Cork
             'openorgs____::38d7097854736583dde879d12dacafca',	-- Brown University
             'openorgs____::57784c9e047e826fefdb1ef816120d92',  --Arts et Métiers ParisTech
             'openorgs____::2530baca8a15936ba2e3297f2bce2e7e',	-- University of Cape Town
             'openorgs____::d11f981828c485cd23d93f7f24f24db1',  -- Technological University Dublin
             'openorgs____::5e6bf8962665cdd040341171e5c631d8',  -- Delft University of Technology
             'openorgs____::846cb428d3f52a445f7275561a7beb5d',  -- University of Manitoba
             'openorgs____::eb391317ed0dc684aa81ac16265de041',	-- Universitat Rovira i Virgili
             'openorgs____::66aa9fc2fceb271423dfabcc38752dc0',  -- Lund University
             'openorgs____::3cff625a4370d51e08624cc586138b2f',	-- IMT Atlantique
             'openorgs____::c0b262bd6eab819e4c994914f9c010e2',   -- National Institute of Geophysics and Volcanology
             'openorgs____::1624ff7c01bb641b91f4518539a0c28a',   -- Vrije Universiteit Amsterdam
             'openorgs____::4d4051b56708688235252f1d8fddb8c1',	 --Iscte - Instituto Universitário de Lisboa
             'openorgs____::ab4ac74c35fa5dada770cf08e5110fab',	 -- Universidade Católica Portuguesa
             'openorgs____::4d4051b56708688235252f1d8fddb8c1',	 -- Iscte - Instituto Universitário de Lisboa
             'openorgs____::5d55fb216b14691cf68218daf5d78cd9',  -- Munster Technological University
             'openorgs____::0fccc7640f0cb44d5cd1b06b312a06b9',  -- Cardiff University
             'openorgs____::8839b55dae0c84d56fd533f52d5d483a',   -- Leibniz Institute of Ecological Urban and Regional Development
             'openorgs____::526468206bca24c1c90da6a312295cf4',	-- Cyprus University of Technology
             'openorgs____::b5ca9d4340e26454e367e2908ef3872f'	-- Alma Mater Studiorum University of Bologna
        )))  foo;