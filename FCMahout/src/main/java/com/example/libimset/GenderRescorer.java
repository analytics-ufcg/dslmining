package com.example.libimset;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;

public class GenderRescorer implements IDRescorer {

    private final FastIDSet men;
    private final FastIDSet women;
    private final FastIDSet rateMoreMen;
    private final FastIDSet rateLessMen;
    private final boolean filterMen;

    public GenderRescorer(FastIDSet men, FastIDSet women, FastIDSet rateMoreMen, FastIDSet rateMoreWomen, long userID, DataModel model) throws TasteException {
        this.men = men;
        this.women = women;
        this.rateMoreMen = rateMoreMen;
        this.rateLessMen = rateMoreMen;
        this.filterMen = ratesMoreMen(userID, model);
    }

    @Override
    public double rescore(long profileID, double originalScore) {
        return isFiltered(profileID) ? Double.NaN : originalScore;
    }

    @Override
    public boolean isFiltered(long profileID) {
        return filterMen ? men.contains(profileID) : women.contains(profileID);
    }

    private boolean ratesMoreMen(long userID, DataModel model) throws TasteException {
        if (rateMoreMen.contains(userID)) {
            return true;
        } else if (rateLessMen.contains(userID)) {
            return false;
        }
        PreferenceArray prefs = model.getPreferencesFromUser(userID);
        int menCount = 0;
        int womenCount = 0;
        for (int i = 0; i < prefs.length(); i++) {
            long profileID = prefs.get(i).getItemID();
            if (men.contains(profileID)) {
                menCount += 1;
            } else if (women.contains(profileID)) {
                womenCount += 1;
            }
        }
        boolean ratesMoreMen = menCount > womenCount;
        if (ratesMoreMen) {
            rateMoreMen.add(userID);
        } else {
            rateLessMen.add(userID);
        }
        return ratesMoreMen;
    }
}
