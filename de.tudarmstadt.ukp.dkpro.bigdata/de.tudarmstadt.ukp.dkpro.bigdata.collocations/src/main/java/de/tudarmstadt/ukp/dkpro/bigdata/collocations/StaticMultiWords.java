package de.tudarmstadt.ukp.dkpro.bigdata.collocations;

public class StaticMultiWords
{

    public static String[][] ambient = { { "coral", "sea" }, { "iwo", "jima" }, { "la", "plata" },
            { "life", "on", "mars" }, { "magic", "mountain" }, { "monte", "carlo" },
            { "out", "of", "control" }, { "purple", "haze" }, { "the", "little", "mermaid" } };
    public static String[] moresque2 = { "the block", "stephen king", "soul food", "cool water",
            "space raiders", "gay bar", "la mancha", "robert watts", "dive bomber", "manor house",
            "indy 500", "new england", "heavy rotation", "jump cut", "dial tone", "fight night",
            "agent blue", "mount huxley", "junk mail", "sean fallon", "double negative",
            "richard tracey", "civil war", "volcanic rock", "space opera", "family reunion",
            "radioactive man", "tai chi", "heart attack", "gorky park", "mark forster",
            "bus driver", "bat boy", "james bond", "special edition", "liquid air",
            "little brother", "magic lantern", "alpha dog", "micro chip", "middle ages",
            "black hole", "radius of curvature", "food for thought", "burden of proof",
            "field of fire", "crown of thorns", "storm in a teacup", "lake of the woods",
            "sign of the cross", "music of the spheres", "trip the light fantastic",
            "twilight of the gods", "survival of the fittest" };
    public static String[] moresque = { "bald eagle", "bermuda triangle", "monte carlo",
            "iron man", "sherlock holmes", "trojan horse", "poison ivy", "ten commandments",
            "flight 93", "ice age", "zero hour", "full moon", "fort recovery", "soul food",
            "mighty mouse", "jurassic park", "indiana university", "heron island", "match point",
            "texas rangers", "brett butler", "courtney cox", "jungle fever", "blood work",
            "harry potter", "guild wars", "john carroll", "black planet", "century 21",
            "coyote ugly", "mortal kombat", "silent hill", "stone cold", "independence day",
            "lemonade stand", "aurora borealis", "agent orange", "babel fish", "far cry",
            "carrot top", "the last supper", "romeo and juliet", "medal of honor",
            "arch of triumph", "dead or alive", "man in black", "heaven and hell", "stand by me",
            "prince of persia", "billy the kid", "dog eat dog", "sense and sensibility",
            "soldier of fortune", "one tree hill", "sisters of mercy", "beauty and the beast",
            "lord of the flies", "battle of the bulge", "the da vinci code", "wizard of oz" };

    public static int getMaximalMWLength()
    {
        return 3;
    }

    public static String[][] getMWEs()
    {
        return ambient;
    }

    public static String matches(String history[])
    {
        for (String[] m : ambient) {
            boolean matched = true;
            for (int i = 0; m.length > i; i++) {
                if (history[history.length - i - 1] == null
                        || !history[history.length - i - 1].equals(m[m.length - i - 1])) {
                    matched = false;
                    break;
                }

            }
            if (matched) {
                StringBuilder sb = new StringBuilder();
                for (String s : m) {
                    sb.append(s);
                    sb.append(" ");
                }
                sb.setLength(sb.length() - 1);
                return sb.toString();

            }
        }
        for (String mwe : moresque) {
            try {
                String[] m = mwe.split(" ");
                boolean matched = true;
                for (int i = 0; m.length > i; i++) {
                    if (history[history.length - i - 1] == null
                            || !history[history.length - i - 1].equals(m[m.length - i - 1])) {
                        matched = false;
                        break;
                    }

                }
                if (matched) {
                    StringBuilder sb = new StringBuilder();
                    for (String s : m) {
                        sb.append(s);
                        sb.append(" ");
                    }
                    sb.setLength(sb.length() - 1);
                    return sb.toString();

                }
            }
            catch (Exception e) {
                System.out
                        .println("exception when processing " + history.length + " length," + mwe);
            }
        }
        return null;
    }
}
