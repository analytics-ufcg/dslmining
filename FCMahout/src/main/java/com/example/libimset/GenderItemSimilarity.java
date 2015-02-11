package com.example.libimset;

import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by igleson on 05/02/15.
 */
public class GenderItemSimilarity implements ItemSimilarity {

    private final FastIDSet men;
    private final FastIDSet women;

    public GenderItemSimilarity(FastIDSet men, FastIDSet women) {
        this.men = men;
        this.women = women;
    }

    @Override
    public double itemSimilarity(long profileID1, long profileID2) throws TasteException {
        Boolean profile1IsMan = isMen(profileID1);
        if (profile1IsMan == null) {
            return 0;
        }

        Boolean profile2IsMan = isMen(profileID2);
        if (profile2IsMan == null) {
            return 0;
        }

        return profileID1 == profileID2 ? 1.0 : -1.0;
    }

    @Override
    public double[] itemSimilarities(long itemID1, long[] itemID2s) throws TasteException {
        return Arrays.stream(itemID2s).mapToDouble(itemID2 -> {
            try {
                return itemSimilarity(itemID1, itemID2);
            } catch (TasteException e) {
                return 0.0;
            }
        }).toArray();
    }

    @Override
    public long[] allSimilarItemIDs(long l) throws TasteException {
        return new long[0];
    }

    @Override
    public void refresh(Collection<Refreshable> collection) {
        //do nothing
    }

    private Boolean isMen(long profileID) {
        if (men.contains(profileID)) {
            return Boolean.TRUE;
        } else if (women.contains(profileID)) {
            return Boolean.FALSE;
        } else {
            return null;
        }
    }
}
