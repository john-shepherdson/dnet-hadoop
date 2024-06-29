package eu.dnetlib.dhp.actionmanager.personentity;

import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;


import java.io.Serializable;

public class Couples implements Serializable {
    Person p ;
    Relation r;

    public Couples() {

    }

    public Person getP() {
        return p;
    }

    public void setP(Person p) {
        this.p = p;
    }

    public Relation getR() {
        return r;
    }

    public void setR(Relation r) {
        this.r = r;
    }

    public static <Tuples> Couples newInstance(Tuple2<Person, Relation> couple){
        Couples c = new Couples();
        c.p = couple._1();
        c.r = couple._2();
        return c;
    }
}
