package com.example.libimset;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;

/**
 * Rescorer based in the fact that doesn't make sense recommend women to a profiles which just like men, and vice-versa.
 */
public class GenderRescorer implements IDRescorer {

    private final FastIDSet men;
    private final FastIDSet women;
    private final FastIDSet rateMoreMen;
    private final FastIDSet rateLessMen;
    private final boolean filterMen;

    /**
     * Constructor for a GenderRescorer
     *
     * @param men           A FastIdSet to place all the male profiles together.
     * @param women         A FastIdSet to place all the female profiles together.
     * @param rateMoreMen   A FastIdSet to place all the profiles which likes more men.
     * @param rateMoreWomen A FastIdSet to place all the profiles which likes more mwoen.
     * @param userID        The id from the user the recommendation will be rescored.
     * @param model         The data for recommendation.
     */
    public GenderRescorer(FastIDSet men, FastIDSet women, FastIDSet rateMoreMen, FastIDSet rateMoreWomen, long userID, DataModel model) throws TasteException {
        this.men = men;
        this.women = women;
        this.rateMoreMen = rateMoreMen;
        this.rateLessMen = rateMoreMen;
        this.filterMen = ratesMoreMen(userID, model);
    }

    /**
     * Rescore a recommendation.
     *
     * @param profileID     Id from the user that will be recommended.
     * @param originalScore Original score for the recommendation.
     * @return A new score for the recommendation.
     */
    @Override
    public double rescore(long profileID, double originalScore) {
        return isFiltered(profileID) ? Double.NaN : originalScore;
    }

    /**
     * Verify if makes sense recommend a user to the user passed in constructor.
     *
     * @param profileID Id from the user that will be recommended.
     * @return true if makes sense recommend this user, false otherwise.
     */
    @Override
    public boolean isFiltered(long profileID) {
        return filterMen ? men.contains(profileID) : women.contains(profileID);
    }

    /**
     * Verify if a user id rates more men.
     *
     * @param userID Id from a user.
     * @param model  The data for recommendation.
     * @return True if the user rates more men, false otherwise.
     */
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
