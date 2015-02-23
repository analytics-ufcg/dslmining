package com.example.libimset;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.util.Arrays;
import java.util.Collection;

/**
 * Similatiry metric based on the fact that people from the same gender have same tastes.
 */
public class GenderItemSimilarity implements ItemSimilarity {

    private final FastIDSet men;
    private final FastIDSet women;

    public GenderItemSimilarity(FastIDSet men, FastIDSet women) {
        this.men = men;
        this.women = women;
    }

    /**
     * Calculates the similarity beetwen two users
     *
     * @param profileID1 User id for one user.
     * @param profileID2 User id for another user.
     * @return A double beetwen -1.0 and 1.0, which -1.0 means no similarity and 1.0 mean maximum similarity
     */
    @Override
    public double itemSimilarity(long profileID1, long profileID2) throws TasteException {
        Boolean profile1IsMan = isMan(profileID1);
        if (profile1IsMan == null) {
            return 0;
        }

        Boolean profile2IsMan = isMan(profileID2);
        if (profile2IsMan == null) {
            return 0;
        }

        return profile1IsMan == profile2IsMan ? 1.0 : -1.0;
    }

    /**
     * Calculates the similarity between a user and a collection of other users.
     *
     * @param profileID1  User id for one user
     * @param profilesIDs Array for user different for the ProfileID1
     * @return A double array containing similarities between -1.0 and 1.0, which -1.0 means no similarity and 1.0 mean
     * maximum similarity
     */
    @Override
    public double[] itemSimilarities(long profileID1, long[] profilesIDs) throws TasteException {
        return Arrays.stream(profilesIDs).mapToDouble(itemID2 -> {
            try {
                return itemSimilarity(profileID1, itemID2);
            } catch (TasteException e) {
                return 0.0;
            }
        }).toArray();
    }

    /**
     * This method won't be implemented
     */
    @Override
    public long[] allSimilarItemIDs(long l) throws TasteException {
        throw new RuntimeException("this method won't be implemented");
    }

    /**
     * This method won't be implemented
     */
    @Override
    public void refresh(Collection<Refreshable> collection) {
        throw new RuntimeException("this method won't be implemented");
    }

    /**
     * Check if a profile is for a man
     *
     * @param profileID User id for one user
     * @return true if the user id is for a profile of man, false otherwise.
     */
    private Boolean isMan(long profileID) {
        if (men.contains(profileID)) {
            return Boolean.TRUE;
        } else if (women.contains(profileID)) {
            return Boolean.FALSE;
        } else {
            return null;
        }
    }
}
