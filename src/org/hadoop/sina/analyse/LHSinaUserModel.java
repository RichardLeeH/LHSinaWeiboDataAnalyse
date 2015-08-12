package org.hadoop.sina.analyse;

/**
 * 数据说明
 * 此部分数据为微博20150801到20150804这几天的部分数据，数据格式如下：
	[
	key,#时间和mid组成的键
	mid,#微博唯一mid
	uid,#用户id
	created_at,#发布时间
	reposts_count#,#转发数
	comments_count,#评论数
	rid,#转发微博的根节点（原始微博的mid）
	pid,#转发微博的上级节点
	text,#微博文本
	source#微博来源
	]
	
	---------------key-------------- ------mid------- ----uid--- ---发布时间----  转发数  评论数  根节点   上级节点  微博文本   微博来源
	[20150803085830-3871678335075786,3871678335075786,2788176594,20150803085830,  0 ,    0 ,    ,    null,    11   ,   Source [url=http://weibo.com/, relationShip=nofollow, name=微博 weibo.com]]
    [20150803085830-3871678335075041,3871678335075041,2591858842,20150803085830,  0,     0 ,    ,    null,    白色连衣裙 给你美的视觉享受[心], ]

 * 
 *
 */
public class LHSinaUserModel 
{
	private static String separator = ",";
	///新浪微博用户ID
	private String mUserID;  
	
	///用户转发微博总数
	private int   mForwardedNum;

	public LHSinaUserModel()
	{
		
	}
	
	public String getmUserID() 
	{
		return mUserID;
	}

	public void setmUserID(String mUserID) 
	{
		this.mUserID = mUserID;
	}

	public int getmForwardedNum() 
	{
		return mForwardedNum;
	}

	public void setmForwardedNum(int mForwardedNum) 
	{
		this.mForwardedNum = mForwardedNum;
	}
	
	public boolean parser(String aContent)
	{
		if(aContent == null || aContent.length() == 0)
		{
			return false;
		}
		
		String[] items = aContent.split(separator);
		if (items.length >= 10)
		{
			this.setmUserID(items[2]);
			this.setmForwardedNum(Integer.valueOf(items[4]));
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////////
	public static void main(String[] args)
	{
		String testData = "[20150803085830-3871678335075786,3871678335075786,2788176594,20150803085830,0,0,,null,11,Source [url=http://weibo.com/, relationShip=nofollow, name=微博 weibo.com]]";
		LHSinaUserModel model = new LHSinaUserModel();
		if (model.parser(testData))
		{
			String userID = model.getmUserID();
			long   num    = model.getmForwardedNum();
			System.out.println(userID);
			System.out.println(num);
		}
	}
}
