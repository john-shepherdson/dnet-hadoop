package eu.dnetlib.dhp.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PacePersonTest {

    @Test
    public void pacePersonTest1(){

        PacePerson p = new PacePerson("Artini, Michele", false);
        assertEquals("Artini",p.getSurnameString());
        assertEquals("Michele", p.getNameString());
        assertEquals("Artini, Michele", p.getNormalisedFullname());
    }

    @Test
    public void pacePersonTest2(){
        PacePerson p = new PacePerson("Michele G. Artini", false);
        assertEquals("Artini, Michele G.", p.getNormalisedFullname());
        assertEquals("Michele G", p.getNameString());
        assertEquals("Artini", p.getSurnameString());
    }

}
